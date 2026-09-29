WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        region,
        sqft,
        DATE_FORMAT(opened_date, 'MMM yyyy') opened_date_label,
        CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END is_tokyo,
        COALESCE(region, 'UNKNOWN') region_filled
    FROM stores
    WHERE region IS NOT NULL
      AND sqft <= 7327
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(is_tokyo) OVER (PARTITION BY region) region_max_is_tokyo,
        FIRST_VALUE(sqft, TRUE) OVER w_grp first_known_sqft
    FROM valid
    WHERE is_tokyo >= 61
    WINDOW w_grp AS (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.sqft,
        s.is_tokyo,
        s.region_max_is_tokyo,
        s.first_known_sqft,
        top1.order_status,
        top1.discount
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status, c.discount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, sqft * region_max_is_tokyo sqft_times_region_max_is_tokyo
    FROM latest_match
    WHERE first_known_sqft <= 4727
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        sqft,
        order_status,
        AVG(sqft_times_region_max_is_tokyo) OVER (PARTITION BY city, order_status ORDER BY region_max_is_tokyo, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) sqft_times_region_max_is_tokyo_avg_5,
        LAG(first_known_sqft, 2, 0) OVER (ORDER BY region_max_is_tokyo, id) prev_first_known_sqft
    FROM filtered
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_status, city ORDER BY prev_first_known_sqft DESC, id) = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.customer_id IN (1, 2)
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        order_status,
        sqft
    FROM kept
) src
PIVOT (
    SUM(sqft)
    FOR order_status IN ('RETURNED' AS returned, 'COMPLETED' AS completed, 'SHIPPED' AS shipped)
);
