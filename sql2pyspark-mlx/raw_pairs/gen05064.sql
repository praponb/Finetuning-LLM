select
    id,
    country,
    customer_id,
    device,
    coalesce(country, 'N/A') as country_filled
from sessions;
