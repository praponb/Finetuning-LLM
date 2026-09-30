with with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(3) over (partition by region, city order by sqft, id) as sqft_bucket,
        row_number() over w_part as event_seq
    from stores
    window w_part as (order by sqft desc, id)
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_windows s
    where exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.customer_id is not null
        )
),
with_latest as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.order_order_date,
        x.discount
    from eligible s
    left join lateral (
            select c.order_date as order_order_date, c.discount
            from orders c
            where c.store_id = s.id
            order by c.discount desc, c.id desc
            limit 1
        ) x on true
)
-- Aggregate with conditional metrics
select
    date_trunc('week', order_order_date) as week_start,
    count(*) as num_rows,
    approx_percentile(discount, 0.5) as discount_p50
from with_latest
group by week_start
order by discount_p50 desc;
