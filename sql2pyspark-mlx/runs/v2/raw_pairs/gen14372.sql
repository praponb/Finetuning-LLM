SELECT id, rating, SUM(rating) OVER (ORDER BY rating, id) AS running_rating FROM suppliers
