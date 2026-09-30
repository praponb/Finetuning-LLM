select account_type, avg_balance, balance_range
from (
    select account_type, round(avg(balance), 2) avg_balance, max(balance) - min(balance) balance_range
    from accounts
    group by account_type
) s
where avg_balance < 29;
