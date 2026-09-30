select stock_qty, length(brand) as brand_length
from products
where launch_date >= '2025-03-01'
order by stock_qty
