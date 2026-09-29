SELECT id, city, opened_date, PERCENT_RANK() OVER (PARTITION BY city ORDER BY opened_date) AS opened_date_pct_rank FROM stores WHERE opened_date < '2025-03-01'
