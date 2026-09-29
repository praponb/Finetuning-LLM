SELECT
    cost,
    order_id,
    CASE WHEN cost >= 83 THEN 'High' WHEN cost >= 43 THEN 'Medium' ELSE 'Low' END AS cost_band,
    DATE_SUB(delivered_date, 46) AS delivered_date_minus_46d
FROM shipments
WHERE delivered_date < '2024-01-01';
