with base as (
    -- Basic cleansing and defaults
    select
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        date_trunc('WEEK', launch_date) launch_date_period,
        coalesce(brand, 'OTHER') brand_filled
    from products
    where brand is distinct from 'Initech'
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        product_name,
        category,
        cost,
        stock_qty,
        launch_date,
        launch_date_period,
        stddev_samp(cost) over (partition by brand) brand_stddev_samp_cost,
        row_number() over (partition by category, brand order by cost desc, id) seq_no
    from base
),
with_latest as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.last_restock,
        latest.inventory_quantity
    from with_windows s
    left join lateral (
            select c.last_restock, c.quantity as inventory_quantity
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(launch_date_period) over (partition by category order by launch_date_period, id) is null
                 or launch_date_period > lag(launch_date_period) over (partition by category order by launch_date_period, id) + interval 7 days then 1 else 0 end as new_group_flag
    from with_latest
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        category,
        stock_qty,
        launch_date_period,
        seq_no,
        last_restock,
        sum(new_group_flag) over (partition by category order by launch_date_period, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        category,
        stock_qty,
        seq_no,
        last_restock,
        burst_id,
        case when stock_qty >= 389 then 'High' when stock_qty >= 193 then 'Medium' else 'Low' end stock_qty_band,
        upper(category) category_upper
    from sessionized
    where burst_id > 3
)
-- Grouped metrics
select
    case when seq_no >= 8 then 'PREMIUM' else 'STANDARD' end as seq_no_tier,
    case when burst_id >= 8 then 'LARGE' else 'SMALL' end as burst_id_tier,
    date_trunc('month', last_restock) as activity_month,
    sum(case when category = 'Grocery' then seq_no else 0 end) as grocery_seq_no,
    sum(burst_id) filter (where category = 'Books') as books_total_burst_id,
    max(seq_no) as max_seq_no,
    bit_or(stock_qty) as stock_qty_bit_or,
    round(sum(burst_id) / nullif(count(case when category = 'Home' then 1 end), 0), 2) as burst_id_per_home,
    rank() over (partition by case when seq_no >= 8 then 'PREMIUM' else 'STANDARD' end order by round(sum(burst_id) / nullif(count(case when category = 'Home' then 1 end), 0), 2) desc) as burst_id_per_home_rank
from filtered
group by case when seq_no >= 8 then 'PREMIUM' else 'STANDARD' end, case when burst_id >= 8 then 'LARGE' else 'SMALL' end, date_trunc('month', last_restock);
