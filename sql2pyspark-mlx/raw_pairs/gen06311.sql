select id, rating, city, supplier_name from suppliers where rating > (select max(rating) from suppliers);
