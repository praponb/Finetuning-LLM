select
    *,
    lag(rating) over w_ordered as prev_rating,
    row_number() over w_ordered as row_seq
from suppliers
window w_ordered as (partition by country order by rating asc, id asc)
