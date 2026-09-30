with recent_shipments as (
    select *, upper(status) as status_upper
    from shipments
    where year(delivered_date) >= 2025 and month(shipped_date) in (2, 7, 9)
),
Agg as (
    select status, date_trunc('MONTH', delivered_date) as delivered_month, count(distinct cost) as unique_orders
    from recent_shipments
    group by status, date_trunc('MONTH', delivered_date)
)
select status, delivered_month, unique_orders
from Agg
order by status desc, delivered_month
limit 1;
