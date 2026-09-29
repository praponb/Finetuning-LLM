SELECT id, status, channel, amount, INITCAP(TRIM(channel)) AS channel_title FROM orders WHERE YEAR(order_date) >= 2023 AND (discount > 0.23 OR status <> 'COMPLETED') ORDER BY amount;
