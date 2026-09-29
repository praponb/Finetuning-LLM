SELECT *, NTILE(5) OVER (ORDER BY amount DESC, id) AS amount_bucket, ROW_NUMBER() OVER w_grp AS event_seq FROM payments WINDOW w_grp AS (PARTITION BY method, order_id ORDER BY amount DESC, id);
