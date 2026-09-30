with base as (
    select *
    from payments
    where method not in ('bank_transfer', 'promptpay') and to_date(paid_at) >= '2023-07-01' and order_id is not null
),
summary as (
    select case when amount >= 1770 then 'high' when amount >= 680 then 'mid' else 'low' end as amount_bucket, count(*) as payment_count
    from base
    group by case when amount >= 1770 then 'high' when amount >= 680 then 'mid' else 'low' end
)
select amount_bucket, payment_count
from summary;
