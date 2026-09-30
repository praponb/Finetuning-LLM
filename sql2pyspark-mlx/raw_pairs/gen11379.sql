select amount
from payments
where to_date(paid_at) >= '2025-06-19'
intersect
select revenue as amount
from store_sales
where product_id in (6, 7)
order by amount desc
