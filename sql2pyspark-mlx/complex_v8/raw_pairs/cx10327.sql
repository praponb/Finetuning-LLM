WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date IS NOT NULL
      AND premium > 2289.0
),
status_stats AS (
    -- Avg premium per status
    SELECT status, AVG(premium) AS avg_premium
    FROM valid
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM valid s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium > g.avg_premium
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM above_average s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status = 'OPEN'
              AND c.adjuster_id IS NOT NULL
        )
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(premium, 0.0) AS premium_filled,
        CONCAT_WS('-', status, product) AS status_product_label
    FROM qualified
    WHERE premium_minus_avg < 2500.0
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    COALESCE(status, 'ALL') AS status,
    ROUND(STDDEV(premium_filled), 2) AS stddev_premium_filled,
    SUM(premium_filled) AS total_premium_filled,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list
FROM labeled
GROUP BY GROUPING SETS ((holder_id, status), (holder_id), (status))
