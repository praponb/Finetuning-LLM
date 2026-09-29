WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(quantity, 0) quantity_filled,
        QUARTER(last_restock) last_restock_quarter
    FROM inventory
    WHERE last_restock > DATE '2023-08-20'
      AND quantity = 201
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(quantity AS STRING) quantity_str,
        quantity - quantity_filled quantity_minus_quantity_filled
    FROM cleaned
    WHERE last_restock >= DATE '2023-06-30'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        quantity_filled,
        quantity_str,
        quantity_minus_quantity_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS latest_rank
        FROM valid
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.last_restock,
        s.quantity_filled,
        d.region,
        d.opened_date,
        d.sqft
    FROM deduped s
    INNER JOIN stores d
        ON s.store_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        last_restock,
        region,
        opened_date,
        sqft,
        SUM(quantity_filled) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_quantity_filled,
        ROW_NUMBER() OVER (PARTITION BY MONTH(opened_date), region ORDER BY quantity_filled DESC, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY region, store_id ORDER BY sqft DESC, id) event_seq,
        AVG(quantity_filled) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) quantity_filled_avg_2
    FROM enriched
    WHERE region IN ('East', 'Central', 'South')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last_restock,
        region,
        sqft,
        cumulative_quantity_filled,
        row_seq,
        MAX(sqft) OVER w_grp running_max_sqft,
        SUM(sqft) OVER w_grp running_sum_sqft,
        MIN(sqft) OVER w_grp running_min_sqft
    FROM scored
    WINDOW w_grp AS (PARTITION BY region ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        running_sum_sqft - running_max_sqft running_sum_sqft_minus_running_max_sqft,
        COALESCE(region, 'DIRECT') region_clean
    FROM metrics
    WHERE last_restock >= TO_DATE('2023-09-06')
      AND region IS NOT NULL
)
SELECT
    id,
    region,
    cumulative_quantity_filled,
    running_max_sqft,
    running_sum_sqft,
    running_sum_sqft_minus_running_max_sqft,
    region_clean
FROM filtered
ORDER BY running_sum_sqft NULLS LAST;
