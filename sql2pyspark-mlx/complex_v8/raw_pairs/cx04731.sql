WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE end_date < DATE '2026-04-25'
),
latest_per_key AS (
    -- Keep the latest row per product
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        product_prefix,
        status_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY premium DESC, id DESC) AS rn
        FROM base_policys
    ) ranked
    WHERE rn = 1
),
screened AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM latest_per_key s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
            WHERE p.status IS NOT NULL
        )
),
grouped AS (
    -- Group-level totals
    SELECT
        product,
        COUNT(*) AS cnt,
        MAX(premium) AS max_premium,
        DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days,
        ROUND(STDDEV(premium), 2) AS stddev_premium,
        ROUND(SUM(premium), 2) AS total_premium
    FROM screened
    WHERE start_date BETWEEN DATE '2024-03-27' AND DATE '2025-03-23'
    GROUP BY product
)
SELECT
    cnt,
    product,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list,
    MIN(max_premium) AS min_max_premium,
    ROUND(SUM(max_premium), 2) AS total_max_premium
FROM grouped
GROUP BY GROUPING SETS ((cnt, product), (cnt), (product), ())
ORDER BY cnt NULLS LAST, product NULLS LAST
