select delivered_quarter, pct_kerry, status_values
from (
    select quarter(delivered_date) as delivered_quarter, round((100 * sum(case when carrier = 'Kerry' then 1 else 0 end)) / count(*), 1) as pct_kerry, concat_ws(', ', sort_array(collect_set(status))) as status_values
    from shipments
    where status not in ('RETURNED', 'LOST', 'DELIVERED')
    group by quarter(delivered_date)
) as sub
where pct_kerry < 74
order by pct_kerry desc
