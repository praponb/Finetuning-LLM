select quarter(last_restock) as last_restock_quarter, product_id, count(*) as cnt
from inventory
group by quarter(last_restock), product_id
having count(*) > 1
order by last_restock_quarter desc, product_id desc;
