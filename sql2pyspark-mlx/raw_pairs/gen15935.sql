select case when p.stock_qty >= 327 then 'high' when p.stock_qty >= 83 then 'mid' else 'low' end stock_qty_bucket, round(stddev(i.quantity), 2) stddev_quantity
from inventory i
left join products p on p.id = i.product_id
where p.brand = 'Stark'
group by case when p.stock_qty >= 327 then 'high' when p.stock_qty >= 83 then 'mid' else 'low' end
having stddev_quantity > 54
order by stock_qty_bucket, stddev_quantity desc
