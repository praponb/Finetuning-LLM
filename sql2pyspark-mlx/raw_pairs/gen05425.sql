WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) AS seq_no,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior
    FROM inventory
    WHERE last_restock BETWEEN '2023-09-11' AND '2025-05-01'
)
-- Multi-level aggregation
SELECT
    CASE WHEN quantity_avg_6_prior >= 263 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_avg_6_prior_tier,
    store_id,
    PERCENTILE_APPROX(seq_no, 0.9) AS seq_no_p90,
    COUNT(*) AS cnt,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    PERCENTILE_APPROX(seq_no, 0.5) AS seq_no_p50,
    STDDEV_SAMP(quantity) AS quantity_stddev
FROM scored
GROUP BY CASE WHEN quantity_avg_6_prior >= 263 THEN 'PREMIUM' ELSE 'STANDARD' END, store_id
HAVING cnt > 1
