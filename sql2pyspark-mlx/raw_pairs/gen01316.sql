with cleaned as (
    select *, round(datediff(shipped_date, delivered_date) / 30, 1) as months_delivered_date_to_shipped_date
    from shipments
    where shipped_date > date '2023-02-17'
      and carrier is distinct from 'FedEx'
),
with_windows as (
    select
        id,
        carrier,
        shipped_date,
        months_delivered_date_to_shipped_date,
        lead(cost, 1) over (order by delivered_date, id) as next_cost,
        avg(cost) over (partition by carrier order by delivered_date, id rows between 6 preceding and 1 preceding) as cost_avg_6_prior,
        first_value(carrier) ignore nulls over (partition by status order by cost, id rows between unbounded preceding and current row) as first_known_carrier
    from cleaned
),
metrics as (
    select
        *,
        lag(next_cost, 2) over (partition by carrier order by shipped_date, id) as prev_next_cost,
        ntile(10) over (partition by month(shipped_date), carrier order by next_cost desc, id) as next_cost_bucket,
        sum(months_delivered_date_to_shipped_date) over (order by next_cost, id rows between unbounded preceding and current row) as cumulative_months_delivered_date_to_shipped_date
    from with_windows
),
scored as (
    select
        id,
        carrier,
        shipped_date,
        months_delivered_date_to_shipped_date,
        next_cost,
        first_known_carrier,
        prev_next_cost,
        cume_dist() over w_ordered as next_cost_bucket_cume_dist,
        ntile(3) over w_ordered_2 as next_cost_bucket_bucket,
        lead(cost_avg_6_prior, 1) over w_ordered_3 as next_cost_avg_6_prior
    from metrics
    window w_ordered as (partition by carrier order by next_cost_bucket nulls first), w_ordered_2 as (partition by carrier order by next_cost_bucket desc, id), w_ordered_3 as (partition by carrier order by cost_avg_6_prior, id)
),
ranked as (
    select *, row_number() over (partition by carrier order by next_cost_bucket_cume_dist desc nulls first, id) as row_num
    from scored
    qualify row_num = 1
)
select
    carrier,
    case when next_cost_bucket_bucket >= 9 then 'PREMIUM' else 'STANDARD' end as next_cost_bucket_bucket_tier,
    sort_array(collect_set(carrier)) as carrier_list,
    sum(next_cost_bucket_bucket) as total_next_cost_bucket_bucket,
    round(sum(next_cost) / nullif(count(case when carrier = 'DHL' then 1 end), 0), 2) as next_cost_per_dhl,
    percentile_approx(next_cost_avg_6_prior, 0.9) as next_cost_avg_6_prior_p90,
    grouping_id() as gid
from ranked
group by grouping sets ((carrier, case when next_cost_bucket_bucket >= 9 then 'PREMIUM' else 'STANDARD' end), (carrier));
