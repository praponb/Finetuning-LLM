SELECT
    id,
    order_id,
    method,
    DATE_FORMAT(paid_at, 'MMM yyyy') AS paid_at_label
FROM payments;
