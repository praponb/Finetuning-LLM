with ranked as (
    select
        id,
        product_id,
        quantity,
        unit_price
    from (
        select *, rank() over (partition by product_id order by unit_price desc nulls last) as row_num
        from order_items
    ) t
    where row_num = 1
),
with_windows as (
    select
        *,
        first_value(quantity) ignore nulls over w_ordered as first_known_quantity,
        corr(unit_price, quantity) over w_recent as corr_unit_price_quantity,
        row_number() over w_recent_2 as seq_no,
        corr(quantity, unit_price) over w_recent as corr_quantity_unit_price,
        dense_rank() over (partition by product_id order by quantity desc) as quantity_dense_rank
    from ranked
    window w_ordered as (partition by product_id order by unit_price, id rows between unbounded preceding and current row), w_recent as (partition by product_id), w_recent_2 as (partition by product_id order by unit_price desc, id)
),
enriched as (
    select
        s.id,
        s.product_id,
        s.quantity,
        s.unit_price,
        b.price,
        b.cost,
        b.brand
    from with_windows s
    left join products b
        on s.product_id <=> b.id
    where b.category in ('Electronics', 'Sports')
),
scored as (
    select
        *,
        ntile(5) over (order by cost desc, id) as cost_bucket,
        count(quantity) over (partition by product_id) as product_id_count_quantity,
        avg(quantity) over (partition by product_id, brand order by unit_price, id rows between 5 preceding and 1 preceding) as quantity_avg_5_prior
    from enriched
)
select
    *,
    first_value(cost, true) over (partition by product_id order by cost_bucket, id rows between unbounded preceding and current row) as first_known_cost,
    avg(quantity_avg_5_prior) over w_hist as quantity_avg_5_prior_avg_3_prior,
    avg(cost_bucket) over w_running as cost_bucket_avg_6
from scored
window w_hist as (order by unit_price, id rows between 3 preceding and 1 preceding), w_running as (partition by product_id order by quantity, id rows between 6 preceding and current row)
