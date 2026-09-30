select customer_id
from accounts
where opened_date between '2023-11-01' and '2025-05-02'
intersect
select customer_id
from accounts
where opened_date <= '2023-07-01';
