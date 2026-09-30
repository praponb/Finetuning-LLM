with filtered as (
    select
        *,
        ceil(units) as units_whole,
        cast(revenue as int) as revenue_int
    from store_sales
    where revenue > 2700
),
latest as (
    select *
    from (
        select *, row_number() over (partition by store_id, product_id order by revenue_int desc, id) as row_num
        from filtered
    ) t
    where row_num = 1
),
metrics as (
    select
        *,
        lag(units) over (partition by product_id order by sale_date, id) as prev_units,
        lead(units) over (partition by product_id order by sale_date, id) as next_units,
        lead(revenue_int) over (partition by product_id order by sale_date, id) as next_revenue_int,
        row_number() over (partition by product_id order by sale_date, id) as row_seq,
        corr(units, revenue) over (partition by store_id) as corr_units_revenue
    from latest
),
with_windows as (
    select
        *,
        sum(corr_units_revenue) over (partition by product_id order by units, id rows between unbounded preceding and current row) as cumulative_corr_units_revenue,
        sum(revenue) over (partition by store_id order by corr_units_revenue, id rows between unbounded preceding and current row) as running_revenue
    from metrics
),
enriched as (
    select
        s.id,
        s.store_id,
        s.product_id,
        s.units,
        s.units_whole,
        s.revenue_int,
        s.next_revenue_int,
        p.cost,
        p.launch_date
    from with_windows s
    left join products p
        on s.product_id = p.id
    where p.price between 1010 and 1040
)
select
    id,
    units,
    next_revenue_int,
    cost
from enriched
