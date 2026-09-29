with valid as (
    -- Keep recent, valid rows
    select
        *,
        case when brand in ('Initech', 'Stark') then 'Group A' else 'Other' end as brand_group,
        stock_qty + cost as stock_qty_plus_cost,
        coalesce(brand, 'DIRECT') as brand_clean
    from products
    where supplier_id is not null
      and launch_date >= date '2023-05-23'
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        product_name,
        price,
        supplier_id,
        brand_clean,
        min(stock_qty) over w_grp as running_min_stock_qty,
        avg(stock_qty) over w_grp as running_avg_stock_qty,
        max(stock_qty) over w_grp as running_max_stock_qty,
        lag(price, 1) over (partition by supplier_id order by price, id) as prev_price
    from valid
    where supplier_id in (23, 28, 33)
    window w_grp as (partition by supplier_id, category order by stock_qty_plus_cost, id rows between unbounded preceding and current row)
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        price,
        supplier_id,
        brand_clean,
        avg(price) over (partition by supplier_id order by running_max_stock_qty, id rows between 3 preceding and current row) as price_avg_3,
        row_number() over (partition by supplier_id order by running_min_stock_qty desc, id) as seq_no,
        first_value(price) ignore nulls over (order by price, id rows between unbounded preceding and current row) as first_known_price,
        avg(running_min_stock_qty) over (partition by supplier_id order by running_min_stock_qty, id rows between 2 preceding and 1 preceding) as running_min_stock_qty_avg_2_prior,
        last_value(running_min_stock_qty) ignore nulls over (order by prev_price, id rows between unbounded preceding and current row) as last_known_running_min_stock_qty
    from with_windows
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by supplier_id order by seq_no desc nulls last, id) as dedup_rank
        from enriched
    ) t
    where dedup_rank = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        supplier_id,
        brand_clean,
        price_avg_3,
        seq_no,
        last_known_running_min_stock_qty,
        avg(price_avg_3) over (partition by supplier_id order by last_known_running_min_stock_qty, id rows between 2 preceding and current row) as last3_avg_price_avg_3,
        sum(price_avg_3) over (partition by supplier_id order by last_known_running_min_stock_qty, id rows between 2 preceding and current row) as last3_sum_price_avg_3,
        max(price_avg_3) over (partition by supplier_id order by last_known_running_min_stock_qty, id rows between 2 preceding and current row) as last3_max_price_avg_3,
        min(price_avg_3) over (partition by supplier_id order by last_known_running_min_stock_qty, id rows between 2 preceding and current row) as last3_min_price_avg_3,
        lead(last_known_running_min_stock_qty, 2) over (partition by supplier_id order by last_known_running_min_stock_qty, id) as next_last_known_running_min_stock_qty
    from latest
)
select
    s.id,
    s.supplier_id,
    s.price_avg_3,
    s.last_known_running_min_stock_qty,
    s.last3_min_price_avg_3,
    (select max(x.cost) from products x where x.supplier_id = s.supplier_id) as max_product_cost
from metrics s
order by last3_min_price_avg_3 desc;
