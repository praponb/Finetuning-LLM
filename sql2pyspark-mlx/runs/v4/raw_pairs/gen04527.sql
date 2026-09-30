select account_type, sum(case when status = 'OPEN' then balance else 0 end) as open_balance
from accounts
where balance >= 23000
group by account_type
order by account_type;
