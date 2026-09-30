SELECT id, rating, rating - LAG(rating, 1) OVER (ORDER BY rating, id) AS rating_change
FROM suppliers
WHERE rating < 3.5
ORDER BY rating_change DESC, 2 DESC;
