select opened_month, balance_range
from (
    select date_trunc('MONTH', opened_date) as opened_month, max(balance) - min(balance) as balance_range
    from accounts
    group by date_trunc('MONTH', opened_date)
) as t
where balance_range < 53
order by balance_range, opened_month
limit 5;
