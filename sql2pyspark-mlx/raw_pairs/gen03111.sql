SELECT id, rating, RANK() OVER (ORDER BY rating DESC) rating_rank FROM suppliers;
