select
    date_format(shipped_date, 'yyyy-MM') as shipped_month,
    status,
    round((100 * sum(case when carrier = 'Kerry' then 1 else 0 end)) / count(*), 1) as pct_kerry,
    max(cost) - min(cost) as cost_range
from shipments
where delivered_date < '2025-07-01'
group by shipped_month, status
order by status desc, shipped_month
limit 20
