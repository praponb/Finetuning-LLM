select tag, count(*) as product_count from products lateral view explode(tags) t as tag where brand in ('Initech', 'Globex') group by tag;
