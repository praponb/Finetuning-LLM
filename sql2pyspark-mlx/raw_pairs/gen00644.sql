WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC NULLS FIRST, id ASC) AS rank_in_group
    FROM stores
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        region,
        sqft,
        LAG(sqft) OVER w_part AS prev_sqft,
        LEAD(sqft) OVER w_part AS next_sqft,
        ROW_NUMBER() OVER w_part AS seq_no,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY YEAR(opened_date), region ORDER BY rank_in_group ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft
    FROM latest
    WINDOW w_part AS (PARTITION BY region ORDER BY opened_date ASC, id ASC)
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.next_sqft,
        s.first_known_sqft,
        top1.inventory_last_restock
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_stores AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_last
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY first_known_sqft DESC, id ASC) = 1
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN next_sqft >= 14448 THEN 'HIGH' ELSE 'LOW' END AS next_sqft_tier,
        MAX(next_sqft) FILTER (WHERE region = 'North') AS north_max_next_sqft,
        ROUND(AVG(next_sqft), 2) AS avg_next_sqft,
        ROUND(AVG(first_known_sqft), 2) AS avg_first_known_sqft,
        ROUND(AVG(first_known_sqft), 2) AS avg_first_known_sqft_2
    FROM latest_stores
    GROUP BY CASE WHEN next_sqft >= 14448 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    next_sqft_tier,
    north_max_next_sqft,
    avg_first_known_sqft,
    avg_first_known_sqft_2
FROM totals
ORDER BY avg_first_known_sqft ASC NULLS FIRST, next_sqft_tier DESC
LIMIT 20;
