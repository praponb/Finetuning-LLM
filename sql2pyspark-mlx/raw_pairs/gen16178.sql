select
    id,
    device,
    browser,
    customer_id,
    upper(device) as device_upper,
    substring(country, 1, 3) as country_prefix
from sessions
where country is not null;
