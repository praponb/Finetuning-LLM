select id, opened_date, ntile(2) over (order by opened_date, id) as bucket
from accounts
