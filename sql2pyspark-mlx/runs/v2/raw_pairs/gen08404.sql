select
    date_trunc('MONTH', opened_date) as opened_month,
    account_type,
    count(case when account_type = 'CREDIT' then 1 end) as credit_count,
    count(balance) as balance_count
from accounts
where month(opened_date) in (1, 8, 9)
group by date_trunc('MONTH', opened_date), account_type
