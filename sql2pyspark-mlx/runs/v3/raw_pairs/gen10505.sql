SELECT id, channel, customer_id, amount, PERCENT_RANK() OVER (PARTITION BY channel, customer_id ORDER BY amount DESC) AS amount_pct_rank FROM orders
