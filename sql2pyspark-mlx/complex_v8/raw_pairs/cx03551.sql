WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label,
        UPPER(TRIM(status)) AS status_clean
    FROM scoped
),
without_matches AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.risk_score >= 0.3
        )
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date DESC), 3) AS start_date_percent_rank,
        NTH_VALUE(start_date, 2) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        NTILE(4) OVER w_all_by_start_date AS start_date_quartile,
        LAG(start_date) OVER w_all_by_start_date AS prev_start_date,
        FIRST_VALUE(start_date) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date
    FROM without_matches
    WINDOW w_all_by_start_date AS (ORDER BY start_date DESC, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT holder_id, start_date, status, start_date_quartile
    FROM with_windows
) src
PIVOT (
    MAX(start_date_quartile)
    FOR status IN ('CANCELLED', 'EXPIRED')
)
ORDER BY holder_id, start_date
