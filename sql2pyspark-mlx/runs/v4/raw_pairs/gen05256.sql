SELECT
    id,
    order_id,
    unit_price,
    PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC) unit_price_pct_rank
FROM order_items;
