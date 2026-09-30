select id, supplier_name from suppliers where rating <= 1.9 union select id, supplier_name from suppliers where city = 'London' order by supplier_name
