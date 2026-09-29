with src as (
    select *
    from accounts
    where balance < 77000 or balance > 78000
),
accounts_stats as (
    select year(opened_date) as opened_year, min(balance) as min_balance
    from src
    group by year(opened_date)
)
select opened_year, min_balance
from accounts_stats
where min_balance > 49000
order by min_balance desc;
