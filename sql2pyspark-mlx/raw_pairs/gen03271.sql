with remaining_keys as (
    -- Set difference on keys
    select id, order_id
    from order_items
    except distinct
    select id, order_id
    from order_items
    where unit_price >= 290
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from order_items s
    inner join remaining_keys k on s.id = k.id and s.order_id = k.order_id
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price * quantity as line_total
    from verified
    where product_id is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        product_id,
        line_total,
        cume_dist() over (order by quantity desc nulls first) as quantity_cume_dist,
        dense_rank() over (partition by product_id order by quantity) as quantity_dense_rank
    from filtered
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by product_id order by quantity_cume_dist desc) as quantity_cume_dist_dense_rank,
        cume_dist() over (partition by product_id order by quantity_dense_rank) as quantity_dense_rank_cume_dist
    from enriched
    where quantity_cume_dist between 0.25 and 0.45
),
joined as (
    -- Attach products attributes
    select
        a.*,
        p.product_name,
        p.stock_qty
    from with_windows a
    left join products p
        on a.product_id = p.id
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, floor(quantity_cume_dist) as quantity_cume_dist_whole
    from joined
    where product_id is null
),
scored as (
    -- Ranking and gap features
    select
        product_id,
        line_total,
        quantity_cume_dist_whole,
        percent_rank() over (order by quantity_cume_dist desc nulls first) as quantity_cume_dist_percent_rank,
        dense_rank() over (order by line_total nulls first) as line_total_dense_rank,
        rank() over (partition by product_id order by quantity_cume_dist nulls last) as quantity_cume_dist_rank
    from base
)
select
    s.product_id,
    s.line_total,
    s.quantity_cume_dist_whole,
    s.quantity_cume_dist_percent_rank,
    s.line_total_dense_rank,
    s.quantity_cume_dist_rank,
    (select sum(x.unit_price) from order_items x where x.product_id = s.product_id) as sum_order_item_unit_price
from scored s
order by quantity_cume_dist_rank nulls last, product_id;
