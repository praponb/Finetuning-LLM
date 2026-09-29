with Src as (
    select *
    from transactions
    where is_flagged = true
),
transactions_stats as (
    select account_id, sum(case when merchant = '7-Eleven' then 1 else 0 end) as 7_eleven_count, sum(case when merchant = '7-Eleven' then amount else 0 end) as 7_eleven_amount
    from Src
    group by account_id
)
select *
from transactions_stats
