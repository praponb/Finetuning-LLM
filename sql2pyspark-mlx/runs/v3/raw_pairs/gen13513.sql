select month(last_restock) as last_restock_month, sum(quantity) as total_quantity, round(sum(quantity) * 100.0 / sum(sum(quantity)) over (), 2) as pct_of_total
from inventory
where quantity = 78
group by month(last_restock);
