SELECT COUNT(rating) AS rating_count, MAX(rating) - MIN(rating) AS rating_range
FROM suppliers
WHERE rating <= 2.1
