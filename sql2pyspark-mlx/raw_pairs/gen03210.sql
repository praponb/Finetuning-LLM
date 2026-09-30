select
    account_type,
    date_format(opened_date, 'yyyy-MM') as opened_month,
    max(balance) as max_balance,
    count(balance) as balance_count,
    min(opened_date) as min_opened_date
from accounts
where customer_id = 32
group by account_type, opened_month
having balance_count < 1;
