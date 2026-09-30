with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, case when category = 'Books' then 1 else 0 end as is_books
    from products
    where launch_date >= to_date('2023-06-22')
),
joined as (
    -- Attach suppliers attributes
    select
        a.*,
        s.country,
        s.city,
        s.supplier_name
    from filtered a
    left join suppliers s
        on a.supplier_id = s.id
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from joined s
    where not exists (
            select 1
            from order_items ch
            where ch.product_id = s.id
              and ch.product_id = 5
        )
),
metrics as (
    -- Ranking and gap features
    select
        *,
        corr(price, cost) over (partition by date_trunc('YEAR', launch_date), brand) as corr_price_cost,
        sum(cost) over (partition by supplier_id order by is_books, id rows between unbounded preceding and current row) as cumulative_cost
    from kept
),
with_dims as (
    -- Attach suppliers attributes
    select
        a.id,
        a.product_name,
        a.category,
        a.brand,
        a.price,
        a.stock_qty,
        a.tags,
        a.supplier_id,
        a.supplier_name,
        a.corr_price_cost,
        d.country as supplier_country
    from metrics a
    left join suppliers d
        on a.supplier_id <=> d.id
)
select
    id,
    category,
    brand,
    price,
    stock_qty,
    supplier_name
from with_dims;
