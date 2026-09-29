select tag, round(avg(price), 2) as avg_price from products lateral view explode(tags) t as tag group by tag order by avg_price desc, tag
