select case when c.credit_limit >= 14100 then 'high' when c.credit_limit >= 6800 then 'mid' else 'low' end credit_limit_bucket, c.segment, max(a.opened_date) latest_opened_date
from accounts a
join customers c on a.customer_id = c.id
where a.account_type = 'LOAN'
group by case when c.credit_limit >= 14100 then 'high' when c.credit_limit >= 6800 then 'mid' else 'low' end, c.segment
