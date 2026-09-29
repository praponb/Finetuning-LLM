WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status
    FROM policies
    WHERE start_date BETWEEN DATE '2024-03-27' AND DATE '2025-05-02'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_status_holder_id_by_start_da AS start_date_rank,
        ROUND(AVG(premium) OVER (PARTITION BY status, YEAR(start_date)), 2) AS status_start_date_year_avg_premium,
        COUNT(*) OVER (PARTITION BY status, YEAR(start_date)) AS rows_in_status_start_date_year,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id ORDER BY start_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        LEAD(premium, 3, 0) OVER w_status_holder_id_by_start_da_2 AS next3_premium,
        MAX(premium) OVER (PARTITION BY status, YEAR(start_date)) AS status_start_date_year_max_premium
    FROM prepared_policys
    WINDOW w_status_holder_id_by_start_da AS (PARTITION BY status, holder_id ORDER BY start_date), w_status_holder_id_by_start_da_2 AS (PARTITION BY status, holder_id ORDER BY start_date, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_id,
        MIN(next3_premium) AS min_next3_premium,
        ROUND(SUM(status_start_date_year_max_premium) / NULLIF(SUM(premium_avg_1_prior), 0), 3) AS status_start_date_year_max_premium_to_premium_av,
        ROUND(SUM(premium) / NULLIF(SUM(status_start_date_year_max_premium), 0), 3) AS premium_to_status_start_date_year_max_premium_ra,
        PERCENTILE_APPROX(status_start_date_year_max_premium, 0.5) AS median_status_start_date_year_max_premium,
        ROUND(100 * COUNT(CASE WHEN status NOT IN ('ACTIVE', 'EXPIRED') THEN 1 END) / COUNT(*), 1) AS pct_status_not_in_active_expired,
        COUNT(*) AS cnt
    FROM ranked
    WHERE status = 'EXPIRED'
    GROUP BY 1
)
SELECT
    holder_id,
    min_next3_premium,
    status_start_date_year_max_premium_to_premium_av,
    premium_to_status_start_date_year_max_premium_ra,
    median_status_start_date_year_max_premium,
    pct_status_not_in_active_expired,
    cnt,
    ABS(cnt - 6) AS cnt_distance
FROM grouped
ORDER BY premium_to_status_start_date_year_max_premium_ra, min_next3_premium DESC
