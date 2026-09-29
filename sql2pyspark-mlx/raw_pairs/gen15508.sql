WITH numbered AS (SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY sale_date DESC) AS rnk FROM store_sales WHERE revenue < 2200) SELECT * FROM numbered WHERE rnk <= 2;
