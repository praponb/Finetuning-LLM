SELECT
    *,
    LAG(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id) AS prev_revenue,
    LEAD(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id) AS next_revenue,
    LEAD(units) OVER (PARTITION BY product_id ORDER BY revenue, id) AS next_units,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) AS event_seq,
    AVG(units) OVER (ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS units_avg_6_prior
FROM store_sales;
