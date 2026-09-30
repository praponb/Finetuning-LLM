select *
from transactions
where account_id in (10, 14, 20) and amount between 2100 and 2200 and account_id in (1, 27, 31);
