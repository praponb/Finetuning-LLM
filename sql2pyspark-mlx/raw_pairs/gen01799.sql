WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(city, ' ', region) AS city_region
    FROM stores
    WHERE region IS NOT NULL
      AND region IS DISTINCT FROM 'North'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(sqft, 2) OVER w_grp AS next_sqft,
        LAG(sqft, 1) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft,
        AVG(sqft) OVER w_part AS sqft_avg_3
    FROM deduped
    WINDOW w_grp AS (PARTITION BY city ORDER BY sqft, id), w_part AS (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
        )
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.opened_date,
        s.city_region,
        s.next_sqft,
        latest.revenue,
        latest.units
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.revenue, c.units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN revenue >= 4000 THEN 'HIGH' ELSE 'LOW' END AS revenue_tier,
    ROUND(AVG(next_sqft), 2) AS avg_next_sqft,
    SUM(CASE WHEN city = 'New York' THEN units ELSE 0 END) AS new_york_units,
    SUM(CASE WHEN city = 'New York' THEN next_sqft ELSE 0 END) AS new_york_next_sqft
FROM latest_match
GROUP BY CASE WHEN revenue >= 4000 THEN 'HIGH' ELSE 'LOW' END
ORDER BY new_york_next_sqft DESC;
