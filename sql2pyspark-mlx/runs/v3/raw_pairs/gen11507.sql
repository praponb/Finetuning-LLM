SELECT id, rating, PERCENT_RANK() OVER (ORDER BY rating) rating_pct_rank FROM suppliers;
