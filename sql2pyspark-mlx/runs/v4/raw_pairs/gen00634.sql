select
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    first_value(browser, true) over (partition by browser order by started_at asc, id asc rows between unbounded preceding and current row) as first_known_browser,
    row_number() over (partition by browser order by started_at desc, id asc) as row_seq,
    last_value(country) ignore nulls over (partition by device order by started_at asc, id asc rows between unbounded preceding and current row) as last_known_country,
    row_number() over (order by started_at desc, id asc) as event_seq,
    row_number() over (partition by browser, customer_id order by started_at asc, id asc) as seq_no
from sessions
