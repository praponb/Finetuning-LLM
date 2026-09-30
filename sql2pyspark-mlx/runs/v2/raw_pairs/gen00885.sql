with ProductsBySupplier as (
    select supplier_id, sum(case when category = 'Home' then price else 0 end) as home_price, max(launch_date) as latest_launch_date
    from products
    group by supplier_id
)
select
    s.supplier_name,
    s.country,
    P.home_price,
    P.latest_launch_date
from suppliers s
left join ProductsBySupplier P on s.id = P.supplier_id;
