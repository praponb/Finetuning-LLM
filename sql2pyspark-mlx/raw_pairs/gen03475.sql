WITH scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY quantity NULLS FIRST) quantity_percent_rank,
        COUNT(quantity) OVER (PARTITION BY store_id, product_id) store_id_count_quantity,
        SUM(quantity) OVER w_part running_quantity
    FROM inventory
    WINDOW w_part AS (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN quantity >= 227 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    BIT_OR(quantity) AS quantity_bit_or,
    PERCENTILE_APPROX(running_quantity, 0.9) AS running_quantity_p90
FROM scored
GROUP BY CASE WHEN quantity >= 227 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING running_quantity_p90 > 0
ORDER BY quantity_stddev NULLS LAST;
