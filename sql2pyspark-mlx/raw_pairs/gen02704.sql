SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id) AS row_num FROM subscriptions) AS r WHERE row_num <= 2;
