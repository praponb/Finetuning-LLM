select
    id,
    customer_id,
    device,
    started_at,
    country,
    row_number() over w_part as row_seq,
    row_number() over w_part_2 as seq_no,
    row_number() over w_part_3 as seq_no_2
from sessions
window w_part as (order by started_at, id), w_part_2 as (partition by customer_id, browser order by started_at, id), w_part_3 as (order by started_at desc, id)
