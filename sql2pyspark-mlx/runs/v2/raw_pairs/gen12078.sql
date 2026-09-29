select status, account_type, min(opened_date) min_opened_date
from accounts
where account_type not in ('CREDIT', 'CHECKING', 'SAVINGS')
group by status, account_type
order by 1, account_type
limit 20;
