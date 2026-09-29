WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date >= DATE '2024-12-03'
      AND end_date < DATE '2026-06-18'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        start_date,
        end_date,
        status,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id, status ORDER BY start_date ASC, id), 3) AS start_date_cume_dist,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id, status ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY status, holder_id ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY status, holder_id ORDER BY start_date DESC, id)) AS days_since_prev_start_date,
        LAG(start_date, 3) OVER (PARTITION BY holder_id, status ORDER BY start_date ASC, id) AS prev3_start_date,
        DENSE_RANK() OVER (PARTITION BY status, holder_id ORDER BY start_date DESC) AS start_date_dense_rank
    FROM cleaned
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type = 'collision'
        )
      AND YEAR(s.second_end_date) = 2025
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        YEAR(second_end_date) AS second_end_date_year,
        COUNT(*) FILTER (WHERE status IN ('ACTIVE', 'LAPSED')) AS n_status_in_active_lapsed,
        SUM(days_since_prev_start_date) AS total_days_since_prev_start_date,
        SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS n_status_cancelled
    FROM qualified
    WHERE YEAR(prev3_start_date) = 2024
    GROUP BY DATE_FORMAT(start_date, 'yyyy-MM'), YEAR(second_end_date)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(start_date_ym), ' ', '_')) AS start_date_ym_slug,
        n_status_cancelled - second_end_date_year AS n_status_cancelled_minus_second_end_date_year,
        INITCAP(TRIM(start_date_ym)) AS start_date_ym_clean
    FROM stats
    WHERE n_status_in_active_lapsed BETWEEN 2 AND 8
),
enriched AS (
    -- Label and score rows
    SELECT *, LOWER(TRIM(start_date_ym_clean)) AS start_date_ym_clean_clean
    FROM derived
    WHERE start_date_ym IS NOT NULL
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY n_status_in_active_lapsed ASC, start_date_ym, second_end_date_year), 3) AS n_status_in_active_lapsed_percent_rank,
        LAST_VALUE(n_status_in_active_lapsed) IGNORE NULLS OVER (ORDER BY n_status_in_active_lapsed ASC, start_date_ym, second_end_date_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_n_status_in_active_lapsed,
        MAX(second_end_date_year) OVER (ORDER BY n_status_in_active_lapsed ASC, start_date_ym, second_end_date_year ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS second_end_date_year_max_3_prior
    FROM enriched
)
SELECT
    start_date_ym,
    n_status_in_active_lapsed,
    total_days_since_prev_start_date,
    start_date_ym_slug,
    n_status_cancelled_minus_second_end_date_year,
    start_date_ym_clean,
    start_date_ym_clean_clean,
    last_known_n_status_in_active_lapsed,
    second_end_date_year_max_3_prior
FROM ranked_2
WHERE n_status_in_active_lapsed BETWEEN 1 AND 6
ORDER BY start_date_ym_slug
