select
    id,
    browser,
    started_at,
    country,
    last_value(country) over (partition by browser order by started_at, id rows between unbounded preceding and unbounded following) as last_country
from sessions;
