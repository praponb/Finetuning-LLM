WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
        )
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        SUBSTRING(store_name, 1, 4) AS store_name_prefix,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM kept
    WHERE region IS NOT NULL
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.amount > 630
        )
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.opened_date,
        s.sqft,
        s.region_clean,
        x.last_restock,
        x.quantity
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        city,
        opened_date,
        sqft,
        region_clean,
        last_restock
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY quantity ASC) AS latest_rank
        FROM latest_match
    ) x
    WHERE latest_rank = 1
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN sqft >= 16146 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
        APPROX_PERCENTILE(sqft, 0.9) AS sqft_p90,
        PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
        BIT_OR(sqft) AS sqft_bit_or,
        ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS sqft_per_london,
        COUNT(*) FILTER (WHERE city = 'Singapore') AS singapore_rows
    FROM deduped
    GROUP BY CASE WHEN sqft >= 16146 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    sqft_tier,
    sqft_p90,
    sqft_quartiles,
    sqft_bit_or,
    sqft_per_london,
    singapore_rows
FROM grouped
ORDER BY sqft_bit_or DESC NULLS LAST, singapore_rows DESC;
