with cleaned as (
    select *, trim(status) as clean_status
    from payments
    where method is not null
      and paid_at < '2024-08-01'
)
select
    case when amount >= 1700 then 'HIGH' else 'LOW' end as amount_tier,
    count(distinct status) as distinct_status,
    count(case when status = 'REFUNDED' then 1 end) as refunded_count,
    approx_percentile(amount, 0.9) as amount_p90
from cleaned
group by 1;
