SELECT * FROM (SELECT id, brand, price, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price, id) AS rnk FROM products) AS t WHERE rnk <= 3;
