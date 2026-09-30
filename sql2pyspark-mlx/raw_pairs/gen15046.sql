select *, row_number() over (partition by city, country order by rating desc, id) as latest_rank
from suppliers
qualify latest_rank = 1;
