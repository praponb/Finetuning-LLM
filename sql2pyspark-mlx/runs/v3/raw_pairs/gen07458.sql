SELECT *, ROUND(rating * 100, 2) AS rating_adjusted FROM suppliers WHERE rating IS NOT NULL;
