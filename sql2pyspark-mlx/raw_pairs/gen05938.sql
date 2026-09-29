select is_flagged, count(amount) as amount_count
from transactions
where amount between 1200 and 3300
group by is_flagged
