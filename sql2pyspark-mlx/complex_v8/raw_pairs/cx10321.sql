WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        DATEDIFF(DATE '2025-12-31', end_date) AS days_since_end_date
    FROM policies
    WHERE product IS NOT NULL
      AND end_date BETWEEN DATE '2025-04-17' AND DATE '2027-03-20'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        days_since_end_date,
        ROUND(premium_bucket / NULLIF(SUM(premium_bucket) OVER (PARTITION BY status), 0), 4) AS premium_bucket_share_of_status,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY holder_id ORDER BY days_since_end_date DESC, id)) AS days_since_prev_end_date,
        LEAD(days_since_end_date, 3) OVER w_holder_id_by_days_since_end AS next3_days_since_end_date,
        LAG(end_date, 3) OVER (PARTITION BY holder_id ORDER BY days_since_end_date DESC, id) AS prev3_end_date,
        RANK() OVER (PARTITION BY holder_id ORDER BY days_since_end_date DESC, id) AS days_since_end_date_rank,
        DENSE_RANK() OVER (PARTITION BY holder_id ORDER BY days_since_end_date DESC, id) AS days_since_end_date_dense_rank
    FROM cleaned
    WINDOW w_holder_id_by_days_since_end AS (PARTITION BY holder_id ORDER BY days_since_end_date DESC, id)
),
holder_id_stats AS (
    -- Max premium per holder_id
    SELECT holder_id, MAX(premium) AS max_premium
    FROM with_windows
    GROUP BY holder_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium,
        ROUND(s.premium - g.max_premium, 2) AS premium_minus_max
    FROM with_windows s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium = g.max_premium
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, premium_bucket_share_of_status, premium_minus_max, premium
    FROM compared
) src
UNPIVOT (
    metric_value FOR measure IN (premium_bucket_share_of_status, premium_minus_max, premium)
) unp
