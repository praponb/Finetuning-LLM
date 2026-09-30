WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        ADD_MONTHS(end_date, 3) AS end_date_plus_3m,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE status <> 'CANCELLED'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date_plus_3m))) AS distinct_holder_id_in_end_date_plus_3m_month,
        DENSE_RANK() OVER (ORDER BY days_start_date_to_end_date DESC) AS days_start_date_to_end_date_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY days_start_date_to_end_date DESC, id), 3) AS days_start_date_to_end_date_cume_dist,
        LEAD(premium, 2) OVER (ORDER BY days_start_date_to_end_date DESC, id) AS next2_premium,
        MAX(premium) OVER (ORDER BY days_start_date_to_end_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_max_3
    FROM base_policys
    WINDOW w_end_date_plus_3m_month AS (PARTITION BY DATE_TRUNC('MONTH', end_date_plus_3m))
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        days_since_start_date,
        distinct_holder_id_in_end_date_plus_3m_month,
        days_start_date_to_end_date_dense_rank,
        days_start_date_to_end_date_cume_dist,
        premium_max_3,
        ROUND(days_start_date_to_end_date / NULLIF(SUM(days_start_date_to_end_date) OVER (PARTITION BY holder_id), 0), 4) AS days_start_date_to_end_date_share_of_holder_id,
        ROUND(days_since_start_date - AVG(days_since_start_date) OVER (PARTITION BY holder_id), 2) AS days_since_start_date_vs_holder_id_avg,
        RANK() OVER w_distinct_holder_id_in_end_da AS premium_max_3_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_holder_id_in_end_date_plus_3m_month, MONTH(end_date_plus_3m) ORDER BY premium_max_3), 3) AS premium_max_3_percent_rank,
        SUM(days_since_start_date) OVER (PARTITION BY holder_id) AS holder_id_sum_days_since_start_date,
        DATEDIFF(end_date, LAG(end_date) OVER w_holder_id_end_date_plus_3m_m) AS days_since_prev_end_date
    FROM sequenced
    WINDOW w_distinct_holder_id_in_end_da AS (PARTITION BY distinct_holder_id_in_end_date_plus_3m_month, MONTH(end_date_plus_3m) ORDER BY premium_max_3), w_holder_id_end_date_plus_3m_m AS (PARTITION BY holder_id, MONTH(end_date_plus_3m) ORDER BY premium ASC, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        QUARTER(end_date) AS end_date_quarter,
        COALESCE(premium_max_3, 0.0) AS premium_max_3_filled
    FROM sequenced_2
),
screened AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.joined_on >= DATE '2024-06-29'
              AND p.region IS NOT NULL
        )
)
SELECT
    id,
    holder_id,
    start_date,
    end_date,
    distinct_holder_id_in_end_date_plus_3m_month,
    days_start_date_to_end_date_dense_rank,
    days_start_date_to_end_date_cume_dist,
    days_start_date_to_end_date_share_of_holder_id,
    days_since_start_date_vs_holder_id_avg,
    premium_max_3_rank,
    holder_id_sum_days_since_start_date,
    days_since_prev_end_date,
    end_date_quarter,
    premium_max_3_filled,
    DATE_TRUNC('MONTH', end_date) AS end_date_month,
    DATE_TRUNC('MONTH', start_date) AS start_date_month
FROM screened
ORDER BY premium_max_3_filled, id
LIMIT 10
