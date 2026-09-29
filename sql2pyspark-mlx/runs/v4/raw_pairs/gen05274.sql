with valid as (
    -- Basic cleansing and defaults
    select *, coalesce(stock_qty, 0) as stock_qty_filled
    from products
    where brand = 'Stark'
),
scored as (
    -- Running and moving aggregates
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        stock_qty_filled,
        avg(stock_qty_filled) over (partition by category order by launch_date, id rows between 3 preceding and 1 preceding) as stock_qty_filled_avg_3_prior,
        min(price) over w_ordered as brand_min_price,
        first_value(price, true) over (partition by brand order by stock_qty, id rows between unbounded preceding and current row) as first_known_price,
        corr(stock_qty, cost) over w_ordered as corr_stock_qty_cost,
        avg(price) over (order by price, id rows between 5 preceding and 1 preceding) as price_avg_5_prior
    from valid
    window w_ordered as (partition by brand)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        avg(price) over (partition by supplier_id order by corr_stock_qty_cost, id rows between 6 preceding and 1 preceding) as price_avg_6_prior,
        ntile(5) over (partition by brand order by stock_qty desc, id) as stock_qty_bucket,
        row_number() over (order by first_known_price desc, id) as event_seq
    from scored
),
stats as (
    -- Aggregate with conditional metrics
    select
        case when event_seq >= 9 then 'PREMIUM' else 'STANDARD' end as event_seq_tier,
        date_trunc('month', launch_date) as activity_month,
        count(*) filter (where brand = 'Acme') as acme_rows,
        bit_and(stock_qty) as stock_qty_bit_and,
        approx_percentile(price, array(0.25, 0.5, 0.75)) as price_quartiles
    from metrics
    group by case when event_seq >= 9 then 'PREMIUM' else 'STANDARD' end, date_trunc('month', launch_date)
)
select
    event_seq_tier,
    activity_month,
    acme_rows,
    price_quartiles
from stats
order by acme_rows desc nulls first;
