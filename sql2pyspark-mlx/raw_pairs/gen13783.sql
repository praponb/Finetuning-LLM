SELECT * FROM (SELECT is_flagged, account_id, MAX(amount) max_amount FROM transactions GROUP BY is_flagged, account_id) agg WHERE max_amount > 3200;
