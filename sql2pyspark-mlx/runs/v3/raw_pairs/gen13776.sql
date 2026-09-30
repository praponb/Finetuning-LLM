SELECT orders.status, payments.order_id, payments.method, COALESCE(orders.channel, 'unassigned') AS channel_filled FROM orders LEFT JOIN payments ON payments.order_id = orders.id;
