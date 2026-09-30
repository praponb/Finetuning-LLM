select p.category, count(distinct ss.revenue) as unique_stores, round(avg(ss.revenue), 2) as avg_revenue
from store_sales ss
inner join products p on p.id = ss.product_id
left join stores s on ss.store_id = s.id
where p.stock_qty between 341 and 388
group by p.category
