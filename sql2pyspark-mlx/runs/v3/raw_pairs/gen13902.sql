select *
from (
    select
        merchant,
        case when merchant in ('7-Eleven', 'Starbucks') then 'core' else 'other' end as merchant_group,
        sum(case when merchant = '7-Eleven' then amount else 0 end) as 7_eleven_amount,
        sum(case when txn_type = 'REFUND' then 1 else 0 end) as refund_count
    from transactions
    group by merchant, case when merchant in ('7-Eleven', 'Starbucks') then 'core' else 'other' end
) as sub
where 7_eleven_amount >= 35
