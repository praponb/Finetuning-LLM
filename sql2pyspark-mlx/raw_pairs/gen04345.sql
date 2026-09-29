with filtered as (
    select *, case when units >= 31 then 'High' when units >= 8 then 'Medium' else 'Low' end as units_level
    from store_sales
    where sale_date > to_date('2023-07-23')
      and units is not null
),
enriched as (
    select
        *,
        round(units / sum(units) over (partition by store_id), 4) as units_share,
        avg(units) over (partition by date_trunc('WEEK', sale_date), product_id order by revenue, id rows between 5 preceding and 1 preceding) as units_avg_5_prior
    from filtered
    where store_id in (3, 4, 6)
),
enriched_store_sales as (
    select
        *,
        max(units_avg_5_prior) over w_ordered as running_max_units_avg_5_prior,
        avg(units_avg_5_prior) over w_ordered as running_avg_units_avg_5_prior,
        count(units_avg_5_prior) over w_ordered as running_count_units_avg_5_prior
    from enriched
    window w_ordered as (partition by product_id order by units_avg_5_prior, id rows between unbounded preceding and current row)
),
metrics as (
    select
        id,
        sale_date,
        revenue,
        running_avg_units_avg_5_prior,
        running_count_units_avg_5_prior,
        count(units) over w_recent as running_count_units,
        sum(units) over w_recent as running_sum_units,
        avg(units) over w_recent as running_avg_units
    from enriched_store_sales
    window w_recent as (partition by product_id, store_id order by revenue, id rows between unbounded preceding and current row)
),
with_windows as (
    select
        id,
        running_avg_units_avg_5_prior,
        running_count_units_avg_5_prior,
        running_avg_units,
        rank() over (order by revenue desc nulls last) as revenue_rank,
        lead(running_avg_units_avg_5_prior, 1) over w_running as next_running_avg_units_avg_5_prior,
        rank() over w_hist as running_sum_units_rank
    from metrics
    window w_running as (order by running_avg_units_avg_5_prior, id), w_hist as (order by running_sum_units nulls last)
)
select
    case when running_count_units_avg_5_prior >= 13 then 'PREMIUM' else 'STANDARD' end as running_count_units_avg_5_prior_tier,
    case when running_avg_units >= 45 then 'PREMIUM' else 'STANDARD' end as running_avg_units_tier,
    percentile_approx(running_avg_units_avg_5_prior, array(0.25, 0.5, 0.75)) as running_avg_units_avg_5_prior_quartiles,
    round(avg(running_sum_units_rank), 2) as avg_running_sum_units_rank,
    count(distinct id) as distinct_ids,
    min(revenue_rank) as min_revenue_rank,
    count(distinct id) as distinct_ids_2
from with_windows
group by case when running_count_units_avg_5_prior >= 13 then 'PREMIUM' else 'STANDARD' end, case when running_avg_units >= 45 then 'PREMIUM' else 'STANDARD' end;
