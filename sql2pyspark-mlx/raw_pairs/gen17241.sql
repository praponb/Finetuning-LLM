select tag_name, round(avg(price), 2) as avg_price from products lateral view explode(tags) t as tag_name where cost is not null group by tag_name order by avg_price desc, tag_name;
