SELECT method, order_id, status, CONCAT_WS('/', method, status) AS method_status_key FROM payments WHERE status = 'SUCCESS' ORDER BY 3
