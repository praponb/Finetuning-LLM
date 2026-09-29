select p.brand, round(stddev(ss.units), 2) as stddev_units
from store_sales ss
join products p on ss.product_id = p.id
join suppliers s on s.id = p.supplier_id
group by p.brand
order by stddev_units desc, brand asc
limit 1
