SELECT * FROM (SELECT id, country, age, email, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age, id) AS pos FROM customers) AS t WHERE pos <= 3 ORDER BY id DESC;
