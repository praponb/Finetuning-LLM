with metrics as (
    -- Running and moving aggregates
    select
        id,
        sale_date,
        units,
        revenue,
        lead(revenue, 1, 0) over (partition by product_id, store_id order by revenue asc, id asc) as next_revenue,
        last_value(units, true) over (partition by product_id order by sale_date asc, id asc rows between unbounded preceding and current row) as last_known_units
    from store_sales
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        sale_date,
        next_revenue,
        last_known_units,
        round(units * 1.07, 2) as units_adjusted,
        round(next_revenue / units, 2) as next_revenue_per_units
    from metrics
    where units <> 30
),
valid_store_sales as (
    -- Keep recent, valid rows
    select
        id,
        next_revenue,
        units_adjusted,
        datediff(current_date(), sale_date) as days_since_sale_date
    from valid
    where sale_date > date '2023-08-07'
),
scored as (
    -- Running and moving aggregates
    select
        *,
        sum(days_since_sale_date) over (order by next_revenue asc, id asc rows between 6 preceding and current row) as last7_sum_days_since_sale_date,
        avg(days_since_sale_date) over (order by next_revenue asc, id asc rows between 6 preceding and current row) as last7_avg_days_since_sale_date,
        max(days_since_sale_date) over (order by next_revenue asc, id asc rows between 6 preceding and current row) as last7_max_days_since_sale_date,
        row_number() over (order by next_revenue desc, id asc) as seq_no
    from valid_store_sales
),
metrics_store_sales as (
    -- Running and moving aggregates
    select
        id,
        next_revenue,
        units_adjusted,
        days_since_sale_date,
        last7_avg_days_since_sale_date,
        lead(seq_no) over w_ordered as next_seq_no,
        lag(seq_no) over w_ordered as prev_seq_no,
        lag(next_revenue) over w_ordered as prev_next_revenue,
        row_number() over w_ordered as row_seq,
        first_value(days_since_sale_date) ignore nulls over w_part as first_known_days_since_sale_date
    from scored
    window w_ordered as (order by seq_no asc, id asc), w_part as (order by last7_max_days_since_sale_date asc, id asc rows between unbounded preceding and current row)
),
report as (
    select
        id,
        next_revenue,
        days_since_sale_date,
        next_seq_no,
        prev_seq_no,
        prev_next_revenue,
        row_seq,
        first_known_days_since_sale_date
    from metrics_store_sales
)
select
    id,
    prev_next_revenue,
    row_seq,
    first_known_days_since_sale_date
from report
order by prev_next_revenue asc nulls last;
