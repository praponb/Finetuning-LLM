WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE premium BETWEEN 1132.1 AND 3719.7
      AND product = 'LIFE'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(start_date) OVER (PARTITION BY holder_id ORDER BY end_date DESC, id) AS prev_start_date,
        SUM(premium) OVER (ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        FIRST_VALUE(end_date) OVER (PARTITION BY holder_id ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id), 2) AS holder_id_avg_premium,
        LAST_VALUE(premium) IGNORE NULLS OVER (ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium
    FROM recent
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product = 'TRAVEL'
              AND p.status = 'CANCELLED'
        )
      AND s.start_date >= DATE '2024-07-19'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(last_known_premium / 1000) * 1000 AS last_known_premium_bucket,
        DATEDIFF(DATE '2025-06-30', prev_start_date) AS days_since_prev_start_date
    FROM without_matches
    WHERE running_premium > 20120.0
),
tagged_2 AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(premium / NULLIF(holder_id_avg_premium, 0), 3) AS premium_per_holder_id_avg_premium,
        ROUND(running_premium * 0.25, 2) AS running_premium_adj
    FROM tagged
    WHERE end_date BETWEEN DATE '2025-11-20' AND DATE '2026-09-19'
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.prev_start_date,
        s.running_premium,
        s.first_end_date,
        s.holder_id_avg_premium,
        s.last_known_premium,
        s.days_since_prev_start_date,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type,
        c.status AS claim_status
    FROM tagged_2 s
    JOIN claims c ON c.policy_id = s.id
)
SELECT
    COALESCE(claim_status, 'ALL') AS claim_status,
    holder_id,
    ARRAY_SORT(COLLECT_SET(claim_status)) AS claim_status_list,
    SUM(last_known_premium) AS total_last_known_premium,
    ROUND(SUM(last_known_premium) / NULLIF(SUM(holder_id_avg_premium), 0), 3) AS last_known_premium_to_holder_id_avg_premium_rati
FROM detail_rows
GROUP BY CUBE(claim_status, holder_id)
ORDER BY claim_status NULLS LAST, holder_id NULLS LAST
