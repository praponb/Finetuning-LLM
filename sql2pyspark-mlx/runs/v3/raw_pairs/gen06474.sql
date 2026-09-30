select s.city, s.country, round(stddev(ss.revenue), 2) as stddev_revenue
from store_sales ss
inner join products p on ss.product_id = p.id
inner join suppliers s on p.supplier_id = s.id
where ss.sale_date < '2023-11-01'
group by s.city, s.country
having round(stddev(ss.revenue), 2) >= 53
order by stddev_revenue;
