SELECT o.channel, o.order_date, p.amount FROM orders o LEFT OUTER JOIN payments p ON o.id = p.order_id
