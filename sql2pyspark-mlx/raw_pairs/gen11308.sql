select
    id,
    customer_id,
    browser,
    started_at,
    row_number() over (partition by customer_id, browser order by started_at, id) started_at_row
from sessions
where country != 'JP'
order by started_at_row;
