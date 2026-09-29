select account_type, max(opened_date) as latest_opened_date
from accounts
where balance between 22000 and 76000 and account_type in ('SAVINGS', 'CREDIT', 'CHECKING')
group by account_type
order by 2, account_type;
