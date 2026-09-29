select id, city, rating, supplier_name from (select *, rank() over (partition by city order by rating) as rn from suppliers) as ranked where rn <= 5 order by supplier_name, city
