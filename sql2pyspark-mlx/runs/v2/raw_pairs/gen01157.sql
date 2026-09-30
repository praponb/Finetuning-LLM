select date_trunc('MONTH', opened_date) as opened_month, sum(case when account_type = 'LOAN' then balance else 0 end) as loan_balance, avg(balance) as avg_balance
from accounts
where account_type in ('LOAN', 'CREDIT', 'CHECKING') and (customer_id is null or account_type = 'CHECKING')
group by 1
