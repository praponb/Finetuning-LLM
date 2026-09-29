select account_type, sum(case when account_type = 'CREDIT' then balance else 0 end) as credit_balance
from accounts
group by account_type
having count(balance) >= 3
order by credit_balance asc, account_type asc
limit 5;
