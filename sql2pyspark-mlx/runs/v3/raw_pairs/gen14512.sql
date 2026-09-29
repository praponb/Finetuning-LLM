select year(last_restock) as last_restock_year, sum(quantity) as total_quantity, round(sum(quantity) * 100.0 / sum(sum(quantity)) over (), 2) as pct_of_total
from inventory
group by year(last_restock)
order by total_quantity desc, pct_of_total asc;
