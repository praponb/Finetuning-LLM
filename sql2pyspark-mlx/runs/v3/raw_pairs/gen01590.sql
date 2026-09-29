WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS rnk FROM payments) SELECT * FROM numbered WHERE rnk <= 5 ORDER BY order_id
