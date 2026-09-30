select *
from (
    select order_id, max(shipped_date) as last_shipped_date, round((100 * sum(case when status = 'IN_TRANSIT' then 1 else 0 end)) / count(*), 1) as pct_in_transit
    from shipments
    where year(shipped_date) = 2023
    group by order_id
) as sub
where pct_in_transit >= 32
