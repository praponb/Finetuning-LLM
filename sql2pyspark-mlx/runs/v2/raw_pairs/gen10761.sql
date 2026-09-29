select id, category, price, tag from products lateral view explode(tags) t as tag where array_contains(tags, 'bestseller')
