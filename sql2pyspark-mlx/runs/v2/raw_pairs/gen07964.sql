SELECT id, rating FROM suppliers WHERE rating > (SELECT MAX(rating) FROM suppliers WHERE city IN ('London', 'Singapore')) ORDER BY rating DESC;
