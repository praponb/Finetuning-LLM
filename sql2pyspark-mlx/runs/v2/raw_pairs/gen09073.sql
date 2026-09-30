select count(amount) as amount_count, count(distinct amount) as unique_accounts
from transactions
where to_date(txn_time) >= '2023-05-21';
