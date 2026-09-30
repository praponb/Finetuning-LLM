select
    carrier,
    case when year(shipped_date) < 2024 then 'before_2024' when year(shipped_date) = 2024 then '2024' else 'after_2024' end as shipped_cohort,
    status,
    sum(case when status = 'IN_TRANSIT' then 1 else 0 end) as in_transit_count
from shipments
group by carrier, case when year(shipped_date) < 2024 then 'before_2024' when year(shipped_date) = 2024 then '2024' else 'after_2024' end, status;
