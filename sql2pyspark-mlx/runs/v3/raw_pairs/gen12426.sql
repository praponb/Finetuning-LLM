SELECT id, rating
FROM suppliers
WHERE rating <= (SELECT MIN(rating) FROM suppliers WHERE country = 'TH')
ORDER BY id;
