select label, round(avg(price), 2) as avg_price from products lateral view explode(tags) t as label where cost < 390 group by label order by avg_price desc, label
