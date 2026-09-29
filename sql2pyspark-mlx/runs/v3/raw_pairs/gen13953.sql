select s.city, count(distinct ss.revenue) as unique_products, round(avg(ss.revenue), 2) as avg_revenue
from store_sales ss
left join products p on ss.product_id = p.id
left join suppliers s on p.supplier_id = s.id
where ss.units between 15 and 24
group by s.city
