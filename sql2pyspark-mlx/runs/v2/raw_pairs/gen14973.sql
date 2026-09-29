select customer_id, account_type, case when balance >= 78000 then 'High' when balance >= 14000 then 'Medium' else 'Low' end as balance_level
from accounts;
