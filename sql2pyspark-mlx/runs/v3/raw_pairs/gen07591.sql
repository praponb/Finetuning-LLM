with supplier_summary as (
    select supplier_id, min(price) as min_price, count(*) as num_products
    from products
    group by supplier_id
)
select s.city, ss.min_price, ss.num_products
from suppliers s
inner join supplier_summary ss on s.id = ss.supplier_id
where ss.num_products >= 6
order by min_price desc
