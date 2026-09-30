select
    id,
    city,
    opened_date,
    rank() over (partition by city order by opened_date) as opened_date_rank
from stores
