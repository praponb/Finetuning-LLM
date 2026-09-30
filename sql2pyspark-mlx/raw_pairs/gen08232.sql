WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        COALESCE(sqft, 0) AS sqft_filled
    FROM stores
    WHERE sqft != 7153
      AND region IS NOT NULL
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.quantity
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(sqft_filled) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_filled_avg_5_prior,
        SUM(quantity) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        LEAD(quantity, 1) OVER (ORDER BY sqft_filled, id) AS next_quantity,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS seq_no
    FROM with_latest
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT DISTINCT
    SELECT id
    FROM stores
    WHERE sqft < 5923
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.id = k.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock <= s.opened_date
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(TRIM(store_name)) AS store_name_normalized,
        LEAST(quantity, sqft) AS least_quantity_sqft,
        COALESCE(region, 'NONE') AS region_filled
    FROM kept
    WHERE opened_date >= TO_DATE('2023-07-29')
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    sqft_filled_avg_5_prior,
    cumulative_quantity,
    sqft_share,
    CASE WHEN region RLIKE '^(Central|East)$' THEN 'EXTENDED' WHEN region RLIKE '^(North|South)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS region_zone
FROM base;
