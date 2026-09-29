with valid as (
    select
        *,
        last_day(launch_date) as launch_date_month_end,
        coalesce(brand, 'DIRECT') as brand_clean
    from products
    where launch_date > date '2023-07-22'
      and brand is not null
),
with_windows as (
    select
        *,
        first_value(stock_qty, true) over w_running as first_known_stock_qty,
        sum(price) over (partition by supplier_id, category order by price, id rows between unbounded preceding and current row) as cumulative_price
    from valid
    window w_running as (partition by supplier_id order by cost, id rows between unbounded preceding and current row)
),
matched_keys as (
    select brand
    from with_windows
    except
    select brand
    from products
    where supplier_id in (8, 13, 14, 18)
),
verified as (
    select s.*
    from with_windows s
    inner join matched_keys k on s.brand = k.brand
)
select
    s.product_name,
    s.category,
    s.price,
    s.cost,
    s.launch_date,
    s.supplier_id,
    s.launch_date_month_end,
    s.brand_clean,
    s.first_known_stock_qty,
    (select avg(t2.price) from products t2 where t2.supplier_id = s.supplier_id) as avg_product_price
from verified s
