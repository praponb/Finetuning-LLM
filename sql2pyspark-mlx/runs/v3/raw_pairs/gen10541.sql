WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS rn FROM stores) SELECT * FROM ranked WHERE rn <= 2 ORDER BY opened_date
