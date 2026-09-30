SELECT discount, channel FROM orders WHERE channel IS NULL AND order_date <= '2024-07-01' AND channel IN ('mobile', 'partner', 'store');
