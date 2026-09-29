select id, supplier_name from suppliers where rating > 2.5 union select id, supplier_name from suppliers where rating <= 3.0;
