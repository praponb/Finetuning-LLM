with cleaned as (
    select *, round(quantity * 1.15, 2) as quantity_scaled
    from order_items
    where product_id in (21, 30, 35)
),
ranked as (
    select *, row_number() over (partition by product_id order by quantity_scaled, id) as latest_rank
    from cleaned
    qualify latest_rank = 1
),
metrics as (
    select
        *,
        ntile(5) over (order by quantity, id) as quantity_bucket,
        ntile(3) over w_running as quantity_bucket_2,
        ntile(3) over (partition by product_id, order_id order by quantity_scaled desc, id) as quantity_scaled_bucket,
        ntile(3) over (partition by order_id order by quantity desc, id) as quantity_bucket_3
    from ranked
    window w_running as (partition by product_id order by quantity desc, id)
),
with_dims as (
    select
        a.*,
        b.amount,
        b.channel,
        b.status
    from metrics a
    inner join orders b
        on a.order_id = b.id
)
select
    *,
    rank() over (partition by order_id order by quantity) as quantity_rank,
    avg(quantity) over (partition by channel order by unit_price, id rows between 2 preceding and 1 preceding) as quantity_avg_2_prior,
    cume_dist() over (partition by status order by quantity_bucket_2 desc nulls last) as quantity_bucket_2_cume_dist,
    percent_rank() over (partition by order_id order by amount desc) as amount_percent_rank
from with_dims;
