select
    location,
    budget,
    case when location in ('New York', 'Chiang Mai') then 'Tier 1' else 'Other' end as location_group,
    initcap(trim(location)) as location_title
from departments
where budget <= 2858000
order by budget desc;
