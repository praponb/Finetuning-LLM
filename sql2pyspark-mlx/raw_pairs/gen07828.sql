with valid as (
    select
        *,
        stock_qty - price as stock_qty_minus_price,
        upper(category) as category_upper
    from products
    where supplier_id is not null
),
scored as (
    select
        id,
        brand,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        category_upper,
        lag(cost) over w_grp as prev_cost,
        lag(stock_qty_minus_price) over w_grp as prev_stock_qty_minus_price,
        lead(stock_qty_minus_price) over w_grp as next_stock_qty_minus_price,
        row_number() over w_grp as event_seq,
        round(cost / sum(cost) over (partition by supplier_id, category), 4) as cost_share
    from valid
    window w_grp as (partition by brand order by stock_qty asc, id asc)
),
with_windows as (
    select
        id,
        brand,
        price,
        cost,
        tags,
        supplier_id,
        prev_stock_qty_minus_price,
        next_stock_qty_minus_price,
        first_value(next_stock_qty_minus_price) ignore nulls over (order by cost_share asc, id asc rows between unbounded preceding and current row) as first_known_next_stock_qty_minus_price,
        avg(cost) over (partition by year(launch_date), supplier_id order by cost asc, id asc rows between 2 preceding and current row) as cost_avg_2,
        lead(event_seq, 2) over w_grp as next_event_seq
    from scored
    window w_grp as (partition by supplier_id order by cost asc, id asc)
),
metrics as (
    select
        *,
        avg(next_event_seq) over w_hist as running_avg_next_event_seq,
        count(next_event_seq) over w_hist as running_count_next_event_seq,
        max(next_event_seq) over w_hist as running_max_next_event_seq,
        ntile(10) over (partition by brand order by cost_avg_2 asc, id asc) as cost_avg_2_bucket
    from with_windows
    window w_hist as (partition by supplier_id, brand order by price asc, id asc rows between unbounded preceding and current row)
),
enriched as (
    select
        *,
        avg(running_max_next_event_seq) over (partition by supplier_id, brand order by cost_avg_2_bucket asc, id asc rows between 6 preceding and 1 preceding) as running_max_next_event_seq_avg_6_prior,
        ntile(5) over (partition by supplier_id order by running_count_next_event_seq asc, id asc) as running_count_next_event_seq_bucket,
        lead(first_known_next_stock_qty_minus_price, 1) over (partition by brand order by running_max_next_event_seq asc, id asc) as next_first_known_next_stock_qty_minus_price
    from metrics
    where running_count_next_event_seq < 7
)
select
    id,
    prev_stock_qty_minus_price,
    cost_avg_2,
    running_avg_next_event_seq,
    cost_avg_2_bucket,
    running_count_next_event_seq_bucket,
    next_first_known_next_stock_qty_minus_price,
    first_value(cost_avg_2, true) over w_grp as first_known_cost_avg_2,
    ntile(3) over (partition by supplier_id order by prev_stock_qty_minus_price asc, id asc) as prev_stock_qty_minus_price_bucket,
    row_number() over (order by price desc, id asc) as row_seq
from enriched
window w_grp as (partition by supplier_id order by next_first_known_next_stock_qty_minus_price asc, id asc rows between unbounded preceding and current row)
