WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status
    FROM policies
    WHERE end_date >= DATE '2025-11-20'
      AND premium >= 2096.6
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(start_date, 2) OVER (ORDER BY premium NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        LAG(premium) OVER w_all_by_premium AS prev_premium,
        LAG(start_date, 3) OVER w_all_by_premium_2 AS prev3_start_date,
        MAX(premium) OVER (PARTITION BY status, holder_id ORDER BY premium DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_max_4,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, status), 2) AS premium_vs_holder_id_status_avg,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY start_date DESC, id) AS rn,
        MAX(premium) OVER (PARTITION BY holder_id) AS holder_id_max_premium,
        COUNT(*) OVER (PARTITION BY holder_id) AS rows_in_holder_id,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, status), 0), 4) AS premium_share_of_holder_id_status,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id ORDER BY start_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id), 0), 4) AS premium_share_of_holder_id,
        MIN(premium) OVER (PARTITION BY holder_id, status) AS holder_id_status_min_premium,
        RANK() OVER (PARTITION BY status, holder_id ORDER BY premium DESC, id) AS premium_rank
    FROM base_policys
    WINDOW w_all_by_premium AS (ORDER BY premium DESC, id), w_all_by_premium_2 AS (ORDER BY premium NULLS LAST, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        premium,
        start_date,
        status,
        second_start_date,
        prev_premium,
        prev3_start_date,
        premium_max_4,
        holder_id_max_premium,
        rows_in_holder_id,
        premium_avg_6_prior,
        premium_share_of_holder_id,
        holder_id_status_min_premium,
        premium_rank,
        TRUNC(start_date, 'MM') AS start_date_month_start
    FROM with_trends
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(prev3_start_date) ORDER BY premium_max_4 ASC), 3) AS premium_max_4_cume_dist,
        NTH_VALUE(holder_id_status_min_premium, 2) OVER (PARTITION BY YEAR(prev3_start_date) ORDER BY premium_max_4 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_id_status_min_premium,
        COUNT(*) OVER w_premium_rank_status AS rows_in_premium_rank_status,
        RANK() OVER w_prev3_start_date_year_by_pre AS premium_max_4_rank,
        LEAD(premium_max_4) OVER (PARTITION BY DATE_TRUNC('MONTH', prev3_start_date), YEAR(start_date_month_start) ORDER BY holder_id_status_min_premium DESC NULLS FIRST, id) AS next_premium_max_4,
        ROUND(AVG(premium_max_4) OVER (PARTITION BY premium_rank, status), 2) AS premium_rank_status_avg_premium_max_4
    FROM tagged
    WINDOW w_premium_rank_status AS (PARTITION BY premium_rank, status), w_prev3_start_date_year_by_pre AS (PARTITION BY YEAR(prev3_start_date) ORDER BY premium_max_4 ASC)
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(prev3_start_date, 'yyyy-MM') AS prev3_start_date_ym,
        DATE_FORMAT(start_date_month_start, 'yyyy-MM') AS start_date_month_start_ym,
        status,
        ROUND(100 * COUNT(CASE WHEN status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_status_is_not_null,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids
    FROM sequenced
    GROUP BY 1, 2, 3
)
SELECT
    prev3_start_date_ym,
    start_date_month_start_ym,
    status,
    pct_status_is_not_null,
    row_count,
    distinct_ids
FROM grouped
ORDER BY prev3_start_date_ym DESC
