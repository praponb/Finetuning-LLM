with deduped as (
    select
        id,
        brand,
        cost,
        stock_qty
    from products
    qualify row_number() over (partition by supplier_id, category order by launch_date desc, id asc) = 1
),
with_windows as (
    select
        *,
        lead(cost) over (partition by brand order by stock_qty asc, id asc) as next_cost,
        lag(cost) over (partition by brand order by stock_qty asc, id asc) as prev_cost,
        lead(stock_qty) over (partition by brand order by stock_qty asc, id asc) as next_stock_qty,
        lag(stock_qty) over (partition by brand order by stock_qty asc, id asc) as prev_stock_qty,
        row_number() over (partition by brand order by stock_qty asc, id asc) as row_seq
    from deduped
),
with_windows_products as (
    select
        *,
        lag(stock_qty) over w_recent as prev_stock_qty_2,
        lead(stock_qty) over w_recent as next_stock_qty_2,
        lag(prev_cost) over w_recent as prev_prev_cost,
        lead(prev_cost) over w_recent as next_prev_cost,
        row_number() over w_recent as row_seq_2,
        first_value(prev_cost, true) over (partition by brand order by stock_qty asc, id asc rows between unbounded preceding and current row) as first_known_prev_cost
    from with_windows
    where next_cost between 210 and 710
    window w_recent as (partition by brand order by prev_cost asc, id asc)
),
ranked as (
    select
        id,
        brand,
        prev_cost,
        next_stock_qty,
        prev_stock_qty,
        prev_stock_qty_2,
        next_stock_qty_2,
        next_prev_cost,
        row_seq_2
    from (
        select *, row_number() over (partition by brand order by row_seq_2 asc, id asc) as dedup_rank
        from with_windows_products
    ) x
    where dedup_rank = 1
),
scored as (
    select
        *,
        sum(prev_cost) over (partition by brand order by prev_stock_qty_2 asc, id asc rows between unbounded preceding and current row) as running_sum_prev_cost,
        avg(prev_cost) over (partition by brand order by prev_stock_qty_2 asc, id asc rows between unbounded preceding and current row) as running_avg_prev_cost,
        min(prev_cost) over (partition by brand order by prev_stock_qty_2 asc, id asc rows between unbounded preceding and current row) as running_min_prev_cost,
        count(prev_cost) over (partition by brand order by prev_stock_qty_2 asc, id asc rows between unbounded preceding and current row) as running_count_prev_cost
    from ranked
)
select
    brand,
    count(*) filter (where brand = 'Umbrella') as umbrella_rows,
    count(*) as cnt,
    approx_percentile(prev_stock_qty_2, 0.9) as prev_stock_qty_2_p90,
    round(sum(next_stock_qty_2) / nullif(count(case when brand = 'Initech' then 1 end), 0), 2) as next_stock_qty_2_per_initech,
    count(case when brand = 'Globex' then 1 end) as globex_count,
    sort_array(collect_set(brand)) as brand_list
from scored
group by brand
having next_stock_qty_2_per_initech > 0
order by prev_stock_qty_2_p90 asc nulls first
