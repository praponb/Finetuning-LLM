select count(*) as account_count
from accounts
where opened_date between '2024-02-22' and '2025-10-01' and opened_date >= '2023-12-01';
