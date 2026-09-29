with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        add_months(shipped_date, 1) as shipped_date_plus_months,
        concat_ws('-', carrier, status) as carrier_status_key
    from shipments
    where order_id in (33, 39)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        avg(cost) over (order by delivered_date, id rows between 2 preceding and current row) as cost_avg_2,
        avg(cost) over (partition by order_id order by delivered_date, id rows between 6 preceding and 1 preceding) as cost_avg_6_prior
    from filtered
    where order_id in (2, 30)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by status order by shipped_date desc nulls first, id) as latest_rank
    from metrics
    qualify latest_rank = 1
),
enriched as (
    -- Ranking and gap features
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_status_key,
        cost_avg_6_prior,
        lag(cost_avg_6_prior) over (partition by carrier order by shipped_date_plus_months, id) as prev_cost_avg_6_prior,
        lag(cost_avg_2) over (partition by carrier order by shipped_date_plus_months, id) as prev_cost_avg_2,
        lead(cost_avg_2) over (partition by carrier order by shipped_date_plus_months, id) as next_cost_avg_2,
        row_number() over (partition by carrier order by shipped_date_plus_months, id) as seq_no,
        round(cost_avg_2 / sum(cost_avg_2) over (partition by month(delivered_date), carrier), 4) as cost_avg_2_share
    from latest
)
-- Multi-level aggregation
select
    status,
    case when cost >= 78 then 'PREMIUM' else 'STANDARD' end as cost_tier,
    approx_percentile(prev_cost_avg_2, 0.95) as prev_cost_avg_2_p95,
    sort_array(collect_set(carrier)) as carrier_list,
    round(avg(seq_no), 2) as avg_seq_no,
    round(sum(cost) / nullif(count(case when status = 'DELIVERED' then 1 end), 0), 2) as cost_per_delivered,
    count(*) as row_count,
    max(seq_no) as max_seq_no
from enriched
group by grouping sets ((status, case when cost >= 78 then 'PREMIUM' else 'STANDARD' end), (status), ())
having max_seq_no < 1000000;
