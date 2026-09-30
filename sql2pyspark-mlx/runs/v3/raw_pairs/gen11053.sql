SELECT id, rating, SUM(rating) OVER (ORDER BY rating, id) AS running_rating
FROM suppliers
WHERE city = 'Singapore'
ORDER BY running_rating ASC;
