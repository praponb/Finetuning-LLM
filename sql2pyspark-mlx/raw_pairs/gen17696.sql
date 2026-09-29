with valid as (
    -- Filter the raw rows and derive helper columns
    select *, price + cost as margin
    from products
    where supplier_id = 9
),
cleaned as (
    -- Basic cleansing and defaults
    select *, concat(product_name, ' ', brand) as product_name_brand
    from valid
    where launch_date > date '2023-09-01'
      and brand is not distinct from 'Initech'
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by category order by price nulls first, id) as latest_rank
    from cleaned
    qualify latest_rank = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        margin,
        product_name_brand,
        latest_rank,
        sum(stock_qty) over (order by price, id rows between unbounded preceding and current row) as running_stock_qty,
        last_value(price, true) over (partition by date_trunc('WEEK', launch_date), brand order by launch_date, id rows between unbounded preceding and current row) as last_known_price
    from deduped
    where supplier_id in (4, 5, 6, 36)
)
-- Grouped metrics
select
    date_trunc('year', launch_date) as year_start,
    count_if(category = 'Grocery') as grocery_events,
    stddev_samp(price) as price_stddev
from scored
group by date_trunc('year', launch_date)
having stddev_samp(price) > 0
order by price_stddev nulls last
