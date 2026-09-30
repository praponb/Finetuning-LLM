select
    *,
    first_value(browser, true) over w_ordered as first_known_browser,
    last_value(browser, true) over (partition by date_trunc('WEEK', started_at), country order by started_at, id rows between unbounded preceding and current row) as last_known_browser,
    row_number() over (order by started_at desc, id) as row_seq
from sessions
window w_ordered as (partition by country, customer_id order by started_at, id rows between unbounded preceding and current row);
