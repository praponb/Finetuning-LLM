select is_flagged, count(distinct amount) unique_accounts
from transactions
group by is_flagged;
