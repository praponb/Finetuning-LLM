select id, supplier_name, country, concat_ws('-', city, country) city_country_key from suppliers where rating < 2.7 order by city_country_key asc, country asc;
