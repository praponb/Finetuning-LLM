with metrics as (
    select
        id,
        start_date,
        monthly_fee,
        sum(monthly_fee) over w_ordered last3_sum_monthly_fee,
        max(monthly_fee) over w_ordered last3_max_monthly_fee,
        min(monthly_fee) over w_ordered last3_min_monthly_fee
    from subscriptions
    where monthly_fee >= 130
    window w_ordered as (partition by status order by start_date, id rows between 2 preceding and current row)
)
select
    case when last3_max_monthly_fee >= 223 then 'LARGE' else 'SMALL' end as last3_max_monthly_fee_tier,
    min(last3_min_monthly_fee) as min_last3_min_monthly_fee,
    approx_percentile(last3_min_monthly_fee, 0.9) as last3_min_monthly_fee_p90,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2
from metrics
group by case when last3_max_monthly_fee >= 223 then 'LARGE' else 'SMALL' end;
