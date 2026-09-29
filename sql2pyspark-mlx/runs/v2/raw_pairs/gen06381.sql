select
    ss.revenue,
    ss.units,
    p.supplier_id,
    p.cost,
    s.country,
    s.rating
from store_sales ss
join products p on ss.product_id = p.id
left join suppliers s on p.supplier_id = s.id
where s.country in ('US', 'SG')
order by 2 desc;
