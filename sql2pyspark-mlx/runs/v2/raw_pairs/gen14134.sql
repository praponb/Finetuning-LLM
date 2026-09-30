select id, units from store_sales where units > (select avg(units) from store_sales where revenue < 3600);
