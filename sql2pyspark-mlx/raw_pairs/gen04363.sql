select is_flagged
from transactions
where merchant = 'Grab' and (to_date(txn_time) < '2024-11-01' or merchant in ('Starbucks', 'Grab', '7-Eleven'));
