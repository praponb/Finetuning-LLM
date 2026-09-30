select
    id,
    customer_id,
    started_at,
    country,
    row_number() over (partition by device, browser order by started_at, id) event_seq,
    last_value(country) ignore nulls over (partition by browser order by started_at, id rows between unbounded preceding and current row) last_known_country,
    last_value(country) ignore nulls over (partition by device order by started_at, id rows between unbounded preceding and current row) last_known_country_2
from sessions;
