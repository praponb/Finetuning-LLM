select brand, sum(price) as total_price, round(sum(price) * 100.0 / sum(sum(price)) over (), 2) as pct_of_total
from products
where stock_qty between 166 and 242
group by brand
