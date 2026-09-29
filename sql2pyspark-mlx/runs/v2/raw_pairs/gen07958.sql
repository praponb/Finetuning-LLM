select store_name, city from stores where store_name is not null intersect select store_name, city from stores where region in ('Central', 'West', 'East') order by store_name;
