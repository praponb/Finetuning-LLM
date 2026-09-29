SELECT *, unit_price - quantity AS line_total, ROUND(unit_price * 1.07, 2) AS unit_price_adjusted FROM order_items;
