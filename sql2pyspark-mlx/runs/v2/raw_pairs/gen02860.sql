select opened_date, trim(account_type) as clean_account_type
from accounts
where opened_date <= '2023-05-20' and balance <= 33000
order by clean_account_type
