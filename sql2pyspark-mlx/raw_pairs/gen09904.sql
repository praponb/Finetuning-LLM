select product_id, sum(revenue) as total_revenue, round(sum(revenue) * 100.0 / sum(sum(revenue)) over (), 2) as pct_of_total
from store_sales
group by product_id
