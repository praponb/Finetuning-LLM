select label, count(*) as product_count from products lateral view explode(tags) t as label where stock_qty between 269 and 349 group by label order by product_count desc, label;
