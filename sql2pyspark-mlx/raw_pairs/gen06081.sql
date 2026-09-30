with valid as (
    select *, cast(price as int) as price_int
    from products
    where brand is not null
),
enriched as (
    select
        e.*,
        p.supplier_name,
        p.country,
        p.rating
    from valid e
    left join suppliers p
        on e.supplier_id = p.id
    where upper(p.supplier_name) like '%ACI%'
),
scored as (
    select
        id,
        product_name,
        brand,
        cost,
        stock_qty,
        launch_date,
        price_int,
        supplier_name,
        country,
        rating,
        row_number() over w_grp as row_seq,
        avg(price) over (partition by country, brand order by cost, id rows between 5 preceding and 1 preceding) as price_avg_5_prior
    from enriched
    window w_grp as (partition by country order by stock_qty desc, id)
),
with_latest as (
    select
        s.*,
        top1.store_sale_revenue,
        top1.store_sale_sale_date
    from scored s
    join lateral (
            select c.revenue as store_sale_revenue, c.sale_date as store_sale_sale_date
            from store_sales c
            where c.product_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) top1 on true
),
with_windows as (
    select
        id,
        product_name,
        brand,
        cost,
        stock_qty,
        launch_date,
        supplier_name,
        rating,
        price_avg_5_prior,
        store_sale_sale_date,
        avg(stock_qty) over w_recent as running_avg_stock_qty,
        count(stock_qty) over w_recent as running_count_stock_qty,
        min(stock_qty) over w_recent as running_min_stock_qty,
        cume_dist() over (partition by brand order by store_sale_revenue desc nulls last) as store_sale_revenue_cume_dist
    from with_latest
    window w_recent as (partition by brand order by stock_qty, id rows between unbounded preceding and current row)
)
select
    date_trunc('month', launch_date) as activity_month,
    count(*) filter (where brand = 'Initech') as initech_rows,
    count(*) as event_count,
    bit_or(stock_qty) as stock_qty_bit_or,
    count(*) filter (where brand = 'Globex') as globex_rows,
    size(collect_set(brand)) as num_brands,
    count(case when brand = 'Initech' then 1 end) as initech_count
from with_windows
group by date_trunc('month', launch_date)
order by num_brands desc
