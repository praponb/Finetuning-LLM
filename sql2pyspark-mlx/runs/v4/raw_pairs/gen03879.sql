select id, city, rating, rank() over (partition by city order by rating) as rnk from suppliers where supplier_name is not null;
