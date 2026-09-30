SELECT id, shipped_date, PERCENT_RANK() OVER (ORDER BY shipped_date DESC) AS shipped_date_pct_rank FROM shipments;
