select id, status, trim(account_type) as clean_account_type
from accounts
where not (year(opened_date) >= 2025 and balance <= 35000 and account_type in ('SAVINGS', 'CHECKING', 'CREDIT'));
