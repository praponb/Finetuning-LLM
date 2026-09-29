with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, cast(balance as int) as balance_int
    from accounts
    where opened_date < '2024-06-25'
)
-- Multi-level aggregation
select
    case when balance_int >= 73 then 'PREMIUM' else 'STANDARD' end as balance_int_tier,
    customer_id,
    round(sum(balance_int) / nullif(count(case when status = 'FROZEN' then 1 end), 0), 2) as balance_int_per_frozen,
    count(distinct status) as distinct_status,
    stddev_samp(balance) as balance_stddev,
    count_if(account_type = 'CHECKING') as checking_events,
    count(*) filter (where status = 'OPEN') as open_rows,
    approx_percentile(balance, array(0.25, 0.5, 0.75)) as balance_quartiles,
    dense_rank() over (partition by case when balance_int >= 73 then 'PREMIUM' else 'STANDARD' end order by round(sum(balance_int) / nullif(count(case when status = 'FROZEN' then 1 end), 0), 2) desc) as balance_int_per_frozen_dense_rank
from filtered
group by case when balance_int >= 73 then 'PREMIUM' else 'STANDARD' end, customer_id;
