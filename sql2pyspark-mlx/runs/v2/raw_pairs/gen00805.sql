select id, country, concat_ws(',', city, supplier_name) as city_supplier_name_key from suppliers where country in ('SG', 'UK');
