with filtered_shipments as (
    select *, round(cost, 0) as cost_rounded
    from shipments
    where carrier != 'Kerry' and shipped_date between '2024-05-01' and '2024-09-01'
),
delivered_month_summary as (
    select date_trunc('MONTH', delivered_date) as delivered_month, sum(case when status = 'LOST' then 1 else 0 end) as lost_count
    from filtered_shipments
    group by date_trunc('MONTH', delivered_date)
)
select delivered_month, lost_count
from delivered_month_summary
where lost_count > 76
order by delivered_month
limit 50
