select city, supplier_name from suppliers where rating <= 1.8 union all select city, supplier_name from suppliers where city is not null order by supplier_name;
