SELECT
    id,
    status,
    customer_id,
    ROUND(shipping_fee / amount, 2) AS amount_with_shipping
FROM orders;
