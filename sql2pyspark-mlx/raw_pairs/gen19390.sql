with enriched as (
    -- Running and moving aggregates
    select
        id,
        store_name,
        opened_date,
        sqft,
        min(sqft) over (partition by city, region order by opened_date, id rows between unbounded preceding and current row) as running_min_sqft,
        sum(sqft) over (partition by city, region order by opened_date, id rows between unbounded preceding and current row) as running_sum_sqft,
        avg(sqft) over (partition by city, region order by opened_date, id rows between unbounded preceding and current row) as running_avg_sqft,
        count(sqft) over (partition by city, region order by opened_date, id rows between unbounded preceding and current row) as running_count_sqft
    from stores
)
-- Aggregate with conditional metrics
select
    case when running_sum_sqft >= 86000 then 'HIGH' else 'LOW' end as running_sum_sqft_tier,
    case when running_min_sqft >= 12300 then 'PREMIUM' else 'STANDARD' end as running_min_sqft_tier,
    count(*) as event_count,
    approx_percentile(running_count_sqft, array(0.25, 0.5, 0.75)) as running_count_sqft_quartiles,
    approx_percentile(running_sum_sqft, 0.9) as running_sum_sqft_p90,
    count(*) as num_rows,
    approx_percentile(running_count_sqft, 0.9) as running_count_sqft_p90,
    count(distinct id) as distinct_ids,
    rank() over (partition by case when running_sum_sqft >= 86000 then 'HIGH' else 'LOW' end order by approx_percentile(running_sum_sqft, 0.9) desc) as running_sum_sqft_p90_rank
from enriched
group by case when running_sum_sqft >= 86000 then 'HIGH' else 'LOW' end, case when running_min_sqft >= 12300 then 'PREMIUM' else 'STANDARD' end
order by running_sum_sqft_p90 desc nulls last
