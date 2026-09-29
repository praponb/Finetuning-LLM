select tag_name, count(*) as product_count from products lateral view explode(tags) t as tag_name group by tag_name order by product_count desc, tag_name;
