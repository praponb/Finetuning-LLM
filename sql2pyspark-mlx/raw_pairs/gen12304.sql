select brand, max(stock_qty) as max_stock_qty
from products
group by brand
order by brand desc
limit 5
