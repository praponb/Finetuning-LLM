SELECT *, UPPER(channel) AS channel_upper, LOWER(channel) AS channel_lower FROM orders WHERE discount < 0.17 AND status != 'RETURNED';
