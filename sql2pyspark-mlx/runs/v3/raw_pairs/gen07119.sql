select supplier_id, max(cost) - min(cost) cost_range, sum(case when category = 'Grocery' then price else 0 end) grocery_price
from products
group by supplier_id;
