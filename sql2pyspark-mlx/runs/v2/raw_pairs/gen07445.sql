select *, case when account_type = 'SAVINGS' then 1 else 0 end as is_savings
from accounts
where opened_date > '2024-01-14'
