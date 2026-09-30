select
    id,
    country,
    lower(country) as country_lower,
    datediff(current_date(), started_at) as days_since_started_at
from sessions
where country is not null;
