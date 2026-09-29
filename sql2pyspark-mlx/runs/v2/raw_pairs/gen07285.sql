select status, ups_count, pct_delivered
from (
    select status, count(case when carrier = 'UPS' then 1 end) as ups_count, round((100 * sum(case when status = 'DELIVERED' then 1 else 0 end)) / count(*), 1) as pct_delivered
    from shipments
    group by status
) as s
where pct_delivered < 30
order by ups_count desc, pct_delivered desc, status
limit 50
