SELECT id, rating, LAG(rating, 1) OVER (ORDER BY rating, id) AS prev_rating
FROM suppliers
ORDER BY id DESC;
