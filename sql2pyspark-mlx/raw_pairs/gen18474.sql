select *
from (
    select date_format(delivered_date, 'yyyy-MM') as delivered_month, count(case when status = 'RETURNED' then 1 end) as returned_count
    from shipments
    where status not in ('DELIVERED', 'IN_TRANSIT')
    group by date_format(delivered_date, 'yyyy-MM')
) as t
where returned_count < 7
order by returned_count desc, delivered_month
limit 5;
