select account_type, count(*) as cnt, max(opened_date) as last_opened_date
from accounts
where status = 'FROZEN'
group by account_type;
