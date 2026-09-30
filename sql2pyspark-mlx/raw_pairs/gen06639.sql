with enriched as (
    -- Ranking and gap features
    select
        *,
        lead(age) over w_recent as next_age,
        lag(age) over w_recent as prev_age,
        lead(credit_limit) over w_recent as next_credit_limit,
        lag(credit_limit) over w_recent as prev_credit_limit,
        row_number() over w_recent as row_seq
    from customers
    window w_recent as (partition by segment, country order by age, id)
)
-- Multi-level aggregation
select
    city,
    case when credit_limit >= 13100 then 'HIGH' else 'LOW' end as credit_limit_tier,
    approx_percentile(credit_limit, array(0.25, 0.5, 0.75)) as credit_limit_quartiles,
    count(case when city = 'Berlin' then 1 end) as berlin_count,
    count(*) as row_count,
    sort_array(collect_set(segment)) as segment_list,
    percentile_approx(credit_limit, array(0.25, 0.5, 0.75)) as credit_limit_quartiles_2,
    corr(credit_limit, age) as corr_credit_limit_age,
    rank() over (partition by city order by count(*) desc) as row_count_rank
from enriched
group by city, case when credit_limit >= 13100 then 'HIGH' else 'LOW' end
order by city desc nulls first;
