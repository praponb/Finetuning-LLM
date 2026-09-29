select
    status,
    carrier,
    round((100 * sum(case when status = 'IN_TRANSIT' then 1 else 0 end)) / count(*), 1) as pct_in_transit,
    sort_array(collect_set(status)) as status_list
from shipments
where year(delivered_date) = 2024 or year(shipped_date) = 2024
group by status, carrier
having round((100 * sum(case when status = 'IN_TRANSIT' then 1 else 0 end)) / count(*), 1) >= 56
order by status desc, pct_in_transit desc;
