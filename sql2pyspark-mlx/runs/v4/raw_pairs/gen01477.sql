with src as (select * from store_sales where revenue >= 3000 and store_id = 4), agg as (select product_id, max(units) as max_units from src group by product_id) select * from agg where max_units > 22
