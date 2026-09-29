WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status = 'CANCELLED'
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY premium ASC, id), 3) AS premium_cume_dist,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY holder_id ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROW_NUMBER() OVER (PARTITION BY product ORDER BY start_date ASC, id) AS rn
    FROM filtered
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.premium_cume_dist,
        s.second_start_date,
        s.rn,
        p.joined_on AS holder_joined_on
    FROM sequenced s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE YEAR(p.joined_on) = 2024
),
labeled AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(product)) AS product_len
    FROM joined_holder
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        product,
        start_date,
        premium_cume_dist,
        second_start_date,
        rn,
        holder_joined_on,
        product_len,
        CASE WHEN product IN ('LIFE', 'HOME') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group,
        CASE WHEN premium >= 3817.5 THEN 'A' WHEN premium >= 2319.8 THEN 'B' ELSE 'C' END AS premium_tier
    FROM labeled
    WHERE second_start_date BETWEEN DATE '2024-04-03' AND DATE '2025-09-24'
),
stats AS (
    -- Summarise per group
    SELECT
        product,
        rn,
        PERCENTILE_APPROX(product_len, 0.5) AS median_product_len,
        MAX(rn) AS max_rn,
        COUNT(*) AS n_rows,
        SUM(premium_cume_dist) AS total_premium_cume_dist,
        COUNT(DISTINCT premium_tier) AS distinct_premium_tiers
    FROM derived
    WHERE product_len > 5
    GROUP BY product, rn
    HAVING COUNT(*) >= 3
)
SELECT
    product,
    n_rows,
    total_premium_cume_dist,
    INITCAP(TRIM(product)) AS product_clean
FROM stats
