WITH scored AS (
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY unit_price ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
        SUM(quantity) OVER (PARTITION BY order_id, product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        SUM(quantity) OVER w_recent AS cumulative_quantity_2,
        FIRST_VALUE(quantity) IGNORE NULLS OVER w_recent AS first_known_quantity,
        AVG(quantity) OVER (ORDER BY unit_price ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior
    FROM order_items
    WHERE order_id IN (11, 14, 38, 39)
    WINDOW w_recent AS (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    product_id,
    APPROX_PERCENTILE(cumulative_quantity, 0.5) AS cumulative_quantity_p50,
    APPROX_PERCENTILE(first_known_quantity, 0.9) AS first_known_quantity_p90
FROM scored
GROUP BY product_id
ORDER BY cumulative_quantity_p50 DESC;
