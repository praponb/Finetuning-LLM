select
    id,
    rating,
    row_number() over w_hist as event_seq,
    ntile(4) over w_hist as rating_bucket,
    round(rating / sum(rating) over (partition by city), 4) as rating_share
from suppliers
window w_hist as (partition by country order by rating asc, id asc);
