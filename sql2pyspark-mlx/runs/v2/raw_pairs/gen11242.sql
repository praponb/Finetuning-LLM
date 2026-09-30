select
    id,
    started_at,
    customer_id,
    browser,
    case when country in ('JP', 'SG') then 'Tier 1' else 'Other' end as country_group
from sessions
where customer_id is not null;
