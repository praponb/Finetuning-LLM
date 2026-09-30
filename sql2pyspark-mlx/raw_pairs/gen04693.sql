WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ARRAY_JOIN(tags, ', ') AS tags_text
    FROM products
    WHERE launch_date > DATE '2023-07-02'
      AND supplier_id IS NOT NULL
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.id,
        e.cost,
        e.supplier_id,
        e.tags_text,
        b.country,
        b.city
    FROM valid e
    LEFT JOIN suppliers b
        ON e.supplier_id <=> b.id
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.order_id = 9
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        cost,
        supplier_id,
        country,
        city,
        AVG(cost) OVER (PARTITION BY country ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost,
        SUM(cost) OVER (PARTITION BY country ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost,
        COUNT(cost) OVER (PARTITION BY country ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost
    FROM not_flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        cost,
        supplier_id,
        city,
        running_count_cost
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, city ORDER BY running_avg_cost, id) AS rank_in_group
        FROM with_windows
    ) x
    WHERE rank_in_group = 1
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN running_count_cost >= 14 THEN 'LARGE' ELSE 'SMALL' END AS running_count_cost_tier,
        SIZE(COLLECT_SET(city)) AS num_cities,
        SIZE(COLLECT_SET(city)) AS num_cities_2,
        COUNT_IF(city = 'Chiang Mai') AS chiang_mai_events,
        ROUND(SUM(running_count_cost) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS running_count_cost_per_singapore,
        SUM(running_count_cost) AS total_running_count_cost,
        APPROX_PERCENTILE(running_count_cost, ARRAY(0.25, 0.5, 0.75)) AS running_count_cost_quartiles
    FROM latest
    GROUP BY CASE WHEN running_count_cost >= 14 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    running_count_cost_tier,
    running_count_cost_per_singapore,
    total_running_count_cost,
    running_count_cost_quartiles
FROM totals
