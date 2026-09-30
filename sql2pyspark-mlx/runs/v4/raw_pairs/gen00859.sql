select id, rating, country, supplier_name from suppliers where rating > (select avg(rating) from suppliers)
