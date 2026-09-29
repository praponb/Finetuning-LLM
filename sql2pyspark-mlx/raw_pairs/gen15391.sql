SELECT * FROM (SELECT country, city, rating FROM suppliers) src PIVOT (AVG(rating) AS value_sum FOR city IN ('New York' AS new_york, 'Bangkok' AS bangkok, 'Singapore' AS singapore));
