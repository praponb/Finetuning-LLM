with supplier_summary as (
    select supplier_id, count(*) as num_products
    from products
    where price <= 860
    group by supplier_id
)
select
    s.id,
    s.city,
    s.rating,
    ss.num_products
from suppliers s
inner join supplier_summary ss on s.id = ss.supplier_id
order by city desc
