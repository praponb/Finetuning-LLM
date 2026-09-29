SELECT dim.channel, COUNT(*) AS num_rows, MAX(s.quantity) AS max_quantity FROM order_items s LEFT JOIN orders dim ON s.order_id = dim.id GROUP BY CUBE (dim.channel);
