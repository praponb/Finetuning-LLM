select
    id,
    country,
    upper(country) country_upper,
    case when device in ('mobile', 'desktop') then 'Tier 1' else 'Other' end device_group
from sessions
where customer_id is null and customer_id = 8 and device in ('tablet', 'mobile')
order by country_upper;
