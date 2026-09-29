SELECT * FROM (SELECT id, city, rating, country, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS row_num FROM suppliers WHERE city = 'New York') AS ranked WHERE row_num = 1;
