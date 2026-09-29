SELECT is_flagged, ROUND(STDDEV(amount), 2) AS stddev_amount FROM transactions GROUP BY is_flagged ORDER BY stddev_amount ASC;
