WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status
    FROM policies
),
status_stats AS (
    -- Median premium per status
    SELECT status, PERCENTILE_APPROX(premium, 0.5) AS median_premium
    FROM cleaned
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_premium,
        ROUND(s.premium - g.median_premium, 2) AS premium_minus_median
    FROM cleaned s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium >= g.median_premium
),
unique_rows AS (
    -- Keep the latest row per status
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        median_premium,
        premium_minus_median
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY premium DESC, id DESC) AS row_num
        FROM outliers
    ) ranked
    WHERE row_num = 1
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM unique_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.start_date >= DATE '2024-11-20'
              AND p.end_date >= DATE '2026-01-23'
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        premium_minus_median,
        ROUND(CUME_DIST() OVER (ORDER BY premium, id), 3) AS premium_cume_dist,
        RANK() OVER (ORDER BY premium) AS premium_rank,
        COUNT(*) OVER w_status_holder_id AS rows_in_status_holder_id,
        premium - LAG(premium) OVER (ORDER BY premium, id) AS premium_change,
        SUM(premium) OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium
    FROM screened
    WINDOW w_status_holder_id AS (PARTITION BY status, holder_id)
)
SELECT
    id,
    premium,
    end_date,
    premium_minus_median,
    premium_cume_dist,
    premium_rank,
    rows_in_status_holder_id,
    premium_change,
    running_premium,
    LEAST(rows_in_status_holder_id, premium_rank) AS least_rows_in_status_holder_id_premium_rank,
    YEAR(end_date) AS end_date_year
FROM windowed
ORDER BY end_date DESC
