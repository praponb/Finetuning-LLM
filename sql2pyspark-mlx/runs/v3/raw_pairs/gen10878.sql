with valid_accounts as (
    select *
    from accounts
    where (balance between 20000 and 60000 and opened_date <= '2024-05-01') or status in ('FROZEN', 'OPEN')
),
AccountTypeSummary as (
    select account_type, status, avg(balance) as avg_balance
    from valid_accounts
    group by account_type, status
)
select *
from AccountTypeSummary
order by 2 desc
