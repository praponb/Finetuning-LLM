WITH base AS (
    SELECT *, LAST_DAY(opened_date) AS opened_date_month_end
    FROM stores
    WHERE region IS NULL
),
matched_keys AS (
    SELECT id, city
    FROM base
    EXCEPT DISTINCT
    SELECT id, city
    FROM stores
    WHERE opened_date < '2023-09-01'
),
kept_rows AS (
    SELECT s.*
    FROM base s
    JOIN matched_keys k ON s.id = k.id AND s.city = k.city
),
not_flagged AS (
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.store_id IN (1, 3, 5, 6)
        )
),
kept AS (
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock < '2024-01-01'
        )
),
filtered AS (
    SELECT *, COALESCE(sqft, 0) AS sqft_filled
    FROM kept
    WHERE city != 'Chiang Mai'
)
SELECT
    CASE WHEN sqft_filled >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_filled_tier,
    SUM(sqft_filled) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_total_sqft_filled,
    MAX(sqft) AS max_sqft,
    COUNT(DISTINCT region) AS distinct_regions
FROM filtered
GROUP BY CASE WHEN sqft_filled >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING MAX(sqft) > 0
ORDER BY chiang_mai_total_sqft_filled DESC;
