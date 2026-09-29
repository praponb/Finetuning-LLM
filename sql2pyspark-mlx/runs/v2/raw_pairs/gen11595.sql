select
    country,
    customer_id,
    started_at,
    initcap(trim(country)) as country_title,
    trim(country) as clean_country
from sessions
where customer_id is null and browser = 'Firefox';
