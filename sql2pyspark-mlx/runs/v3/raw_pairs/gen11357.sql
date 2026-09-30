with SupplierSummary as (
    select supplier_id, sum(case when category = 'Grocery' then price else 0 end) as grocery_price, max(launch_date) as latest_launch_date
    from products
    group by supplier_id
)
select
    s.id,
    s.city,
    s.country,
    S.grocery_price,
    S.latest_launch_date
from suppliers s
inner join SupplierSummary S on s.id = S.supplier_id
