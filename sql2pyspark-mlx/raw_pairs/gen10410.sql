select id, product_name, category, tag from products lateral view explode(tags) t as tag where array_contains(tags, 'premium');
