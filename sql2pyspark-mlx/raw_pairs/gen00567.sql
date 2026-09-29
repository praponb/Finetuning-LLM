with base as (
    select *
    from transactions
    where merchant = '7-Eleven'
),
MerchantSummary as (
    select merchant, max(amount) as max_amount
    from base
    group by merchant
)
select *
from MerchantSummary
where max_amount < 1200
order by merchant
