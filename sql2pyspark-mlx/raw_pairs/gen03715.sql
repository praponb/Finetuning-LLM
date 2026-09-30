WITH base AS (
    SELECT *, ADD_MONTHS(last_restock, 6) AS last_restock_plus_months
    FROM inventory
    WHERE quantity BETWEEN 102 AND 174
),
enriched AS (
    SELECT
        *,
        MAX(quantity) OVER (PARTITION BY product_id) AS product_id_max_quantity,
        AVG(quantity) OVER (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY quantity, id) AS quantity_bucket,
        PERCENT_RANK() OVER (PARTITION BY YEAR(last_restock_plus_months) ORDER BY quantity NULLS LAST) AS quantity_percent_rank
    FROM base
),
enriched_inventory AS (
    SELECT
        id,
        last_restock_plus_months,
        quantity_bucket,
        SUM(quantity_avg_3_prior) OVER w_ordered AS last4_sum_quantity_avg_3_prior,
        COUNT(quantity_avg_3_prior) OVER w_ordered AS last4_count_quantity_avg_3_prior,
        AVG(quantity_avg_3_prior) OVER w_ordered AS last4_avg_quantity_avg_3_prior,
        MIN(quantity_avg_3_prior) OVER w_ordered AS last4_min_quantity_avg_3_prior
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT *, ROUND(last4_min_quantity_avg_3_prior, 2) AS last4_min_quantity_avg_3_prior_rounded
    FROM enriched_inventory
    WHERE quantity_bucket >= 6
),
totals AS (
    SELECT
        CASE WHEN last4_avg_quantity_avg_3_prior >= 208 THEN 'LARGE' ELSE 'SMALL' END AS last4_avg_quantity_avg_3_prior_tier,
        DATE_TRUNC('QUARTER', last_restock_plus_months) AS quarter_start,
        CASE WHEN last4_min_quantity_avg_3_prior_rounded >= 89 THEN 'HIGH' ELSE 'LOW' END AS last4_min_quantity_avg_3_prior_rounded_tier,
        APPROX_PERCENTILE(last4_count_quantity_avg_3_prior, ARRAY(0.25, 0.5, 0.75)) AS last4_count_quantity_avg_3_prior_quartiles,
        PERCENTILE_APPROX(last4_min_quantity_avg_3_prior_rounded, ARRAY(0.25, 0.5, 0.75)) AS last4_min_quantity_avg_3_prior_rounded_quartiles,
        APPROX_PERCENTILE(last4_avg_quantity_avg_3_prior, ARRAY(0.25, 0.5, 0.75)) AS last4_avg_quantity_avg_3_prior_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS event_count,
        GROUPING_ID() AS grouping_level
    FROM cleaned
    GROUP BY CUBE (CASE WHEN last4_avg_quantity_avg_3_prior >= 208 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', last_restock_plus_months), CASE WHEN last4_min_quantity_avg_3_prior_rounded >= 89 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    last4_avg_quantity_avg_3_prior_tier,
    last4_count_quantity_avg_3_prior_quartiles,
    last4_min_quantity_avg_3_prior_rounded_quartiles,
    last4_avg_quantity_avg_3_prior_quartiles,
    distinct_ids,
    event_count,
    grouping_level
FROM totals
ORDER BY grouping_level NULLS LAST
