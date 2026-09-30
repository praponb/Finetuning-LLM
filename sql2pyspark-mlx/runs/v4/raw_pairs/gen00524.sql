SELECT segment FROM customers WHERE country = 'UK' INTERSECT SELECT segment FROM customers WHERE city IN ('London', 'Tokyo', 'Chiang Mai') ORDER BY segment
