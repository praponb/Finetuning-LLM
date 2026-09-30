select quarter(c.signup_date) as signup_quarter, max(a.balance) as max_balance
from accounts a
left join customers c on a.customer_id = c.id
where a.customer_id in (5, 11, 29, 32)
group by quarter(c.signup_date)
order by max_balance;
