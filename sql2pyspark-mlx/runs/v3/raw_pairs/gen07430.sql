with supplier_summary as (
    select supplier_id, max(price) - min(price) as price_range
    from products
    group by supplier_id
)
select s.city, s.supplier_name, ss.price_range
from suppliers s
join supplier_summary ss on s.id = ss.supplier_id
