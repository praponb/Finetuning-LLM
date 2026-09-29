with cleaned as (
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue * units as revenue_per_unit
    from store_sales
    where sale_date > date '2023-03-25'
      and sale_date < '2023-09-01'
),
metrics as (
    select
        id,
        product_id,
        count(units) over w_hist as product_id_count_units,
        avg(revenue) over (order by sale_date, id rows between 5 preceding and current row) as revenue_avg_5,
        avg(units) over (partition by product_id order by units, id rows between 5 preceding and 1 preceding) as units_avg_5_prior
    from cleaned
    window w_hist as (partition by product_id, store_id)
),
joined as (
    select
        e.id,
        e.product_id,
        e.units_avg_5_prior,
        p.launch_date,
        p.stock_qty
    from metrics e
    left join products p
        on e.product_id = p.id
),
ranked as (
    select *, row_number() over (partition by product_id order by stock_qty desc, id) as dedup_rank
    from joined
    qualify dedup_rank = 1
),
remaining_keys as (
    select id, product_id
    from ranked
    except
    select id, product_id
    from store_sales
    where sale_date <= '2023-04-01'
),
verified as (
    select s.*
    from ranked s
    inner join remaining_keys k on s.id = k.id and s.product_id = k.product_id
),
base as (
    select
        product_id,
        units_avg_5_prior,
        stock_qty,
        dedup_rank,
        round(dedup_rank * 0.9, 2) as dedup_rank_adjusted,
        date_add(launch_date, 15) as launch_date_plus_15d
    from verified
    where product_id in (2, 3, 4, 8)
)
select
    s.product_id,
    s.stock_qty,
    s.dedup_rank,
    s.dedup_rank_adjusted,
    s.launch_date_plus_15d,
    (select min(t2.revenue) from store_sales t2 where t2.product_id = s.product_id) as min_store_sale_revenue
from base s;
