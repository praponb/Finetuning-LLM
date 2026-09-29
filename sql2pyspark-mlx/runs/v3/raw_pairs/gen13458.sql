select *, concat_ws('/', supplier_name, city) supplier_name_city_key from suppliers order by rating, id limit 1
