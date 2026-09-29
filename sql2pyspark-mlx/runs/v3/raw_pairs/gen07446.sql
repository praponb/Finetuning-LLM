select supplier_name from suppliers where city is not null union select supplier_name from suppliers where supplier_name not like 'T%';
