select
    case when s.rating >= 4.3 then 'high' when s.rating >= 1.6 then 'mid' else 'low' end as rating_bucket,
    s.country,
    count(distinct p.cost) as unique_suppliers,
    count(p.stock_qty) as stock_qty_count
from products p
inner join suppliers s on s.id = p.supplier_id
where s.supplier_name is null
group by case when s.rating >= 4.3 then 'high' when s.rating >= 1.6 then 'mid' else 'low' end, s.country
order by unique_suppliers desc, rating_bucket desc;
