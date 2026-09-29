WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE product NOT IN ('HOME', 'TRAVEL')
      AND status IN ('CANCELLED', 'EXPIRED')
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(days_start_date_to_end_date, 3) OVER (PARTITION BY product, status ORDER BY days_start_date_to_end_date, id) AS next3_days_start_date_to_end_date,
        ROUND(CUME_DIST() OVER w_product_status_by_days_start_2, 3) AS days_start_date_to_end_date_cume_dist,
        ROUND(AVG(days_start_date_to_end_date) OVER (PARTITION BY holder_id, status), 2) AS holder_id_status_avg_days_start_date_to_end_date
    FROM valid
    WINDOW w_product_status_by_days_start_2 AS (PARTITION BY product, status ORDER BY days_start_date_to_end_date)
),
summary AS (
    -- Summarise per group
    SELECT
        product,
        status,
        COUNT(DISTINCT status) AS distinct_status,
        MIN(start_date) AS first_start_date,
        COUNT(*) AS cnt
    FROM with_trends
    GROUP BY product, status
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE cnt > (SELECT AVG(cnt) FROM summary)
)
SELECT product, status, metric, reading
FROM (
    SELECT product, status, distinct_status, cnt
    FROM filtered
) src
UNPIVOT (
    reading FOR metric IN (distinct_status AS distinct_status_v, cnt AS cnt_v)
) unp
