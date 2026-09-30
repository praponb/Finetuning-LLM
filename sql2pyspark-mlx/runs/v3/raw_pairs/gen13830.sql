select customer_id, month(opened_date) as opened_month, count(case when account_type = 'CHECKING' then 1 end) as checking_count
from accounts
group by customer_id, month(opened_date)
order by 3 desc, opened_month desc
