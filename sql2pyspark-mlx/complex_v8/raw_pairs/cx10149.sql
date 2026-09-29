WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix,
        DATE_TRUNC('QUARTER', end_date) AS end_date_quarter,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM policies
    WHERE (end_date >= DATE '2025-12-05' OR start_date IS NOT NULL)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM valid
    WHERE premium > (SELECT AVG(premium) FROM valid)
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.status_prefix,
        s.end_date_quarter,
        s.product_prefix,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment
    FROM screened s
    JOIN policyholders p ON p.id = s.holder_id AND LOWER(p.holder_name) LIKE '%gu%'
),
screened_2 AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM combined_details_holder s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND HOUR(c.reported_at) BETWEEN 10 AND 13
        )
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(premium) OVER w_holder_id_holder_segment, 2) AS holder_id_holder_segment_avg_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, holder_segment), 2) AS premium_vs_holder_id_holder_segment_avg,
        COUNT(*) OVER (PARTITION BY holder_id, holder_segment) AS rows_in_holder_id_holder_segment,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, holder_segment), 0), 4) AS premium_share_of_holder_id_holder_segment,
        ROUND((UNIX_TIMESTAMP(end_date_quarter) - UNIX_TIMESTAMP(LAG(end_date_quarter) OVER (PARTITION BY holder_segment ORDER BY end_date_quarter DESC, id))) / 60, 1) AS minutes_since_prev_end_date_quarter,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium
    FROM screened_2
    WINDOW w_holder_id_holder_segment AS (PARTITION BY holder_id, holder_segment)
),
screened_3 AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date < DATE '2025-09-14'
        )
      AND s.status_prefix IS NOT NULL
),
holder_segment_stats AS (
    -- Avg minutes_since_prev_end_date_quarter per holder_segment
    SELECT holder_segment, AVG(minutes_since_prev_end_date_quarter) AS avg_minutes_since_prev_end_date_quarter
    FROM screened_3
    GROUP BY holder_segment
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_minutes_since_prev_end_date_quarter,
        ROUND(s.minutes_since_prev_end_date_quarter - g.avg_minutes_since_prev_end_date_quarter, 2) AS minutes_since_prev_end_date_quarter_minus_avg
    FROM screened_3 s
    JOIN holder_segment_stats g ON g.holder_segment = s.holder_segment
    WHERE s.minutes_since_prev_end_date_quarter > g.avg_minutes_since_prev_end_date_quarter
)
SELECT id, field, reading
FROM (
    SELECT id, premium_vs_holder_id_holder_segment_avg, holder_id_holder_segment_avg_premium, avg_minutes_since_prev_end_date_quarter
    FROM vs_group
) src
UNPIVOT (
    reading FOR field IN (premium_vs_holder_id_holder_segment_avg, holder_id_holder_segment_avg_premium, avg_minutes_since_prev_end_date_quarter)
) unp
ORDER BY id, field
