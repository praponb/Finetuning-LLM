select
    s.id,
    s.country,
    s.city,
    t.price_range
from suppliers s
left outer join (select supplier_id, max(price) - min(price) as price_range
    from products
    group by supplier_id) t on s.id = t.supplier_id
where t.price_range > 61
order by s.city desc;
