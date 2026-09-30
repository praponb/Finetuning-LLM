SELECT
    id,
    amount,
    status,
    COALESCE(discount, 0) discount_filled,
    ROUND(shipping_fee / amount, 2) amount_with_shipping
FROM orders
WHERE (channel NOT IN ('store', 'web', 'mobile') AND shipping_fee BETWEEN 34 AND 37) OR channel != 'partner'
