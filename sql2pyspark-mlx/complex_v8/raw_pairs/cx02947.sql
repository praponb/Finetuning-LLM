WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        ABS(premium - 2844.1) AS premium_distance
    FROM policies
    WHERE status IN ('ACTIVE', 'LAPSED', 'CANCELLED')
      AND start_date < DATE '2025-05-02'
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.premium_distance,
        p.risk_score AS holder_risk_score
    FROM recent s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_holder_id AS rows_in_holder_id,
        RANK() OVER w_holder_id_status_by_end_date AS end_date_rank,
        LAST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        LAG(end_date, 3) OVER w_all_by_end_date AS prev3_end_date,
        ROUND(AVG(premium_distance) OVER (ORDER BY end_date NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_distance_avg_2,
        COUNT(*) OVER (PARTITION BY holder_id, status) AS rows_in_holder_id_status,
        LEAD(premium_distance, 1, 0) OVER w_all_by_premium_distance AS next_premium_distance,
        DENSE_RANK() OVER (ORDER BY end_date NULLS LAST, id) AS end_date_dense_rank
    FROM combined_details
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_holder_id_status_by_end_date AS (PARTITION BY holder_id, status ORDER BY end_date, id), w_all_by_end_date AS (ORDER BY end_date NULLS LAST, id), w_all_by_premium_distance AS (ORDER BY premium_distance, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        end_date,
        status,
        holder_risk_score,
        rows_in_holder_id,
        end_date_rank,
        last_known_end_date,
        premium_distance_avg_2,
        rows_in_holder_id_status,
        ROUND(end_date_rank - AVG(end_date_rank) OVER (PARTITION BY end_date_dense_rank, DATE_TRUNC('MONTH', last_known_end_date)), 2) AS end_date_rank_vs_end_date_dense_rank_last_known_,
        ROUND(next_premium_distance / NULLIF(SUM(next_premium_distance) OVER (PARTITION BY end_date_dense_rank, DATE_TRUNC('MONTH', last_known_end_date)), 0), 4) AS next_premium_distance_share_of_end_date_dense_ra,
        RANK() OVER (ORDER BY prev3_end_date) AS prev3_end_date_rank,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY end_date_dense_rank, DATE_TRUNC('MONTH', last_known_end_date))) AS distinct_status_in_end_date_dense_rank_last_know,
        SUM(next_premium_distance) OVER (PARTITION BY end_date_dense_rank, DATE_TRUNC('MONTH', last_known_end_date)) AS end_date_dense_rank_last_known_end_date_month_su,
        ROUND(premium_distance - AVG(premium_distance) OVER w_end_date_dense_rank_last_kno, 2) AS premium_distance_vs_end_date_dense_rank_last_kno,
        MAX(next_premium_distance) OVER (PARTITION BY MONTH(prev3_end_date) ORDER BY rows_in_holder_id, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_premium_distance_max_2
    FROM sequenced
    WINDOW w_end_date_dense_rank_last_kno AS (PARTITION BY end_date_dense_rank, DATE_TRUNC('MONTH', last_known_end_date))
)
-- Final projection
SELECT
    id,
    end_date,
    holder_risk_score,
    rows_in_holder_id,
    end_date_rank,
    last_known_end_date,
    next_premium_distance_share_of_end_date_dense_ra,
    premium_distance_vs_end_date_dense_rank_last_kno,
    ROUND(premium_distance_avg_2 / NULLIF(rows_in_holder_id, 0), 3) AS premium_distance_avg_2_per_rows_in_holder_id,
    DATE_FORMAT(last_known_end_date, 'yyyy-MM-dd') AS last_known_end_date_label
FROM with_windows
ORDER BY end_date_rank DESC NULLS FIRST
