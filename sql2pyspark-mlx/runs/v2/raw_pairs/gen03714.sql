select customer_id, status, start_date, date_format(end_date, 'yyyy-MM-dd') end_date_label from subscriptions where plan in ('enterprise', 'standard');
