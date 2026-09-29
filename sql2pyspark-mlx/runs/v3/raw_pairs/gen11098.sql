WITH numbered AS (SELECT id, brand, price, launch_date, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS rn FROM products) SELECT * FROM numbered WHERE rn <= 3;
