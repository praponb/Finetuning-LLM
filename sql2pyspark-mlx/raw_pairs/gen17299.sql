WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE last_restock >= DATE '2023-08-21'
      AND last_restock <= '2023-06-01'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY days_since_last_restock, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior,
        STDDEV_SAMP(quantity) OVER (PARTITION BY product_id) AS product_id_stddev_samp_quantity,
        AVG(days_since_last_restock) OVER (ORDER BY days_since_last_restock, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS days_since_last_restock_avg_5_prior
    FROM filtered
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity_avg_5_prior DESC, id) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        product_id_stddev_samp_quantity,
        days_since_last_restock_avg_5_prior,
        LAG(product_id_stddev_samp_quantity, 1) OVER (PARTITION BY store_id, product_id ORDER BY quantity_avg_5_prior, id) AS prev_product_id_stddev_samp_quantity,
        NTILE(3) OVER (PARTITION BY product_id, store_id ORDER BY quantity_avg_5_prior, id) AS quantity_avg_5_prior_bucket,
        SUM(days_since_last_restock_avg_5_prior) OVER (PARTITION BY YEAR(last_restock), product_id) AS last_restock_year_sum_days_since_last_restock_avg_5_prior,
        SUM(product_id_stddev_samp_quantity) OVER (PARTITION BY YEAR(last_restock), store_id ORDER BY quantity_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_product_id_stddev_samp_quantity,
        FIRST_VALUE(quantity_avg_5_prior) IGNORE NULLS OVER w_running AS first_known_quantity_avg_5_prior
    FROM latest
    WINDOW w_running AS (PARTITION BY DATE_TRUNC('month', last_restock), product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(days_since_last_restock_avg_5_prior) OVER w_grp AS next_days_since_last_restock_avg_5_prior,
        LAG(days_since_last_restock_avg_5_prior) OVER w_grp AS prev_days_since_last_restock_avg_5_prior,
        LEAD(first_known_quantity_avg_5_prior) OVER w_grp AS next_first_known_quantity_avg_5_prior,
        LAG(first_known_quantity_avg_5_prior) OVER w_grp AS prev_first_known_quantity_avg_5_prior,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM scored
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY last_restock, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY prev_product_id_stddev_samp_quantity DESC NULLS LAST, id) = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    ROUND(AVG(prev_days_since_last_restock_avg_5_prior), 2) AS avg_prev_days_since_last_restock_avg_5_prior,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    ROUND(AVG(days_since_last_restock_avg_5_prior), 2) AS avg_days_since_last_restock_avg_5_prior,
    ROUND(AVG(first_known_quantity_avg_5_prior), 2) AS avg_first_known_quantity_avg_5_prior,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM deduped
GROUP BY year_start
ORDER BY num_rows DESC
