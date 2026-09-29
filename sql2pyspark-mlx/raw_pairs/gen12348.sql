with valid as (
    -- Filter the raw rows and derive helper columns
    select *, last_day(last_restock) as last_restock_month_end
    from inventory
    where last_restock between '2023-10-01' and '2025-10-05'
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        product_id,
        last_restock_month_end,
        lead(quantity) over w_ordered as next_quantity,
        lag(quantity) over w_ordered as prev_quantity,
        row_number() over w_ordered as event_seq
    from valid
    window w_ordered as (partition by product_id order by last_restock_month_end asc, id asc)
),
kept_keys as (
    -- Set difference on keys
    select id, product_id
    from with_windows
    except all
    select id, product_id
    from inventory
    where product_id in (3, 5, 7, 9)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join kept_keys k on s.id = k.id and s.product_id = k.product_id
),
joined as (
    -- Attach products attributes
    select a.*, p.category
    from verified a
    left join products p
        on a.product_id = p.id
),
valid_inventory as (
    -- Keep recent, valid rows
    select
        *,
        lower(category) as category_lower,
        round(prev_quantity * 100, 2) as prev_quantity_scaled
    from joined
    where category is not null
),
with_windows_inventory as (
    -- Running and moving aggregates
    select
        *,
        avg(next_quantity) over (partition by product_id) as product_id_avg_next_quantity,
        max(prev_quantity) over (partition by product_id) as product_id_max_prev_quantity,
        rank() over (partition by product_id order by prev_quantity asc nulls first) as prev_quantity_rank
    from valid_inventory
    where prev_quantity between 165 and 202
)
select
    s.id,
    s.product_id,
    s.event_seq,
    s.category,
    s.prev_quantity_scaled,
    s.product_id_max_prev_quantity,
    s.prev_quantity_rank,
    case when category rlike '^(Grocery|Sports)$' then 'GROUP_B' else 'OTHER_ZONE' end as category_zone,
    (select max(x.units) from store_sales x where x.product_id = s.product_id) as max_store_sale_units
from with_windows_inventory s
order by category_zone asc
