with kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from orders s
    where not exists (
            select 1
            from payments c
            where c.order_id = s.id
              and c.paid_at < '2024-12-01'
        )
)
-- Aggregate with conditional metrics
select
    customer_id,
    status,
    date_trunc('quarter', order_date) as quarter_start,
    avg(discount) filter (where status = 'COMPLETED') as completed_avg_discount,
    count(*) as event_count,
    corr(discount, shipping_fee) as corr_discount_shipping_fee,
    grouping(customer_id) as gid
from kept
group by grouping sets ((customer_id, status, date_trunc('quarter', order_date)), (customer_id, status), ())
having event_count > 1;
