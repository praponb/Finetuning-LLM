select id, rating, rank() over (order by rating desc) as rnk from suppliers;
