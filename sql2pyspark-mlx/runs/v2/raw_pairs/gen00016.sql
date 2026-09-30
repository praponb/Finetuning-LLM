select id, rating, supplier_name from suppliers where city = 'Singapore' and city is null and supplier_name like '%Parts%' order by id desc;
