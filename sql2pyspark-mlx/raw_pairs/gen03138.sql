with kept_keys as (
    select id, plan
    from subscriptions
    except all
    select id, plan
    from subscriptions
    where status = 'EXPIRED'
),
reconciled as (
    select s.*
    from subscriptions s
    join kept_keys k on s.id = k.id and s.plan = k.plan
)
select
    case when monthly_fee >= 195 then 'LARGE' else 'SMALL' end as monthly_fee_tier,
    approx_percentile(monthly_fee, array(0.25, 0.5, 0.75)) as monthly_fee_quartiles,
    min(monthly_fee) as min_monthly_fee,
    min(monthly_fee) as min_monthly_fee_2,
    count(*) filter (where plan = 'basic') as basic_rows,
    count(*) as num_rows
from reconciled
group by case when monthly_fee >= 195 then 'LARGE' else 'SMALL' end
order by num_rows desc nulls first;
