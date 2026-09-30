WITH numbered AS (SELECT id, country, age, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age, id) AS pos FROM customers) SELECT id, country, age FROM numbered WHERE pos <= 3;
