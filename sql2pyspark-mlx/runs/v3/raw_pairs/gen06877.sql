select supplier_id, count(price) as price_count, sum(cost) as total_cost
from products
where stock_qty is null
group by supplier_id
order by supplier_id, total_cost desc
limit 5
