with scored as (
    -- Window metrics over several different partitions
    select
        *,
        avg(quantity) over w_part last7_avg_quantity,
        count(quantity) over w_part last7_count_quantity,
        sum(quantity) over w_part last7_sum_quantity,
        sum(quantity) over (partition by product_id order by last_restock, id rows between unbounded preceding and current row) running_quantity
    from inventory
    window w_part as (partition by store_id order by quantity, id rows between 6 preceding and current row)
),
scored_inventory as (
    -- Window metrics over several different partitions
    select
        *,
        lead(last7_sum_quantity, 1, 0) over (partition by store_id order by last7_sum_quantity, id) next_last7_sum_quantity,
        lag(last7_count_quantity, 2) over (partition by month(last_restock), product_id order by quantity, id) prev_last7_count_quantity,
        lag(last7_count_quantity, 2) over (partition by store_id order by running_quantity, id) prev_last7_count_quantity_2
    from scored
    where last7_count_quantity < 8
),
joined as (
    -- Attach stores attributes
    select
        s.*,
        p.region,
        p.sqft
    from scored_inventory s
    left join stores p
        on s.store_id = p.id
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        lead(next_last7_sum_quantity) over (partition by store_id order by quantity, id) next_next_last7_sum_quantity,
        lag(prev_last7_count_quantity) over (partition by store_id order by quantity, id) prev_prev_last7_count_quantity,
        lead(prev_last7_count_quantity) over (partition by store_id order by quantity, id) next_prev_last7_count_quantity,
        row_number() over (partition by store_id order by quantity, id) event_seq
    from joined
),
stats as (
    -- Aggregate with conditional metrics
    select
        date_trunc('month', last_restock) as activity_month,
        product_id,
        round(sum(prev_last7_count_quantity) / nullif(count(case when region = 'North' then 1 end), 0), 2) as prev_last7_count_quantity_per_north,
        stddev_samp(quantity) as quantity_stddev,
        percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
        count(*) filter (where region = 'South') as south_rows,
        count(case when region = 'South' then 1 end) as south_count
    from enriched
    group by activity_month, product_id
)
select
    activity_month,
    product_id,
    quantity_stddev,
    south_rows,
    south_count
from stats;
