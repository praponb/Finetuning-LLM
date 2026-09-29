WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product NOT IN ('TRAVEL', 'AUTO')
      AND start_date >= DATE '2024-07-19'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        start_date,
        status,
        NTILE(3) OVER (PARTITION BY MONTH(start_date) ORDER BY end_date NULLS LAST, id) AS end_date_tercile,
        NTILE(5) OVER (ORDER BY end_date, id) AS end_date_quintile,
        LAG(end_date) OVER (ORDER BY end_date, id) AS prev_end_date,
        ROW_NUMBER() OVER (PARTITION BY product ORDER BY start_date DESC NULLS FIRST, id) AS rn,
        ROW_NUMBER() OVER (PARTITION BY MONTH(start_date) ORDER BY end_date NULLS LAST, id) AS rn_2,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY holder_id, status ORDER BY start_date, id)) AS days_since_prev_start_date,
        LAST_VALUE(end_date) OVER (PARTITION BY status, holder_id ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        ROUND(CUME_DIST() OVER (PARTITION BY product ORDER BY end_date ASC, id), 3) AS end_date_cume_dist,
        LEAD(start_date, 3) OVER w_all_by_end_date AS next3_start_date,
        LEAD(start_date, 3) OVER (PARTITION BY MONTH(start_date) ORDER BY end_date NULLS LAST, id) AS next3_start_date_2
    FROM filtered_policys
    WINDOW w_all_by_end_date AS (ORDER BY end_date, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label,
        end_date_quintile + rn_2 AS end_date_quintile_plus_rn_2
    FROM with_trends
)
SELECT
    id,
    holder_id,
    start_date,
    status,
    end_date_quintile,
    prev_end_date,
    rn,
    rn_2,
    days_since_prev_start_date,
    last_end_date,
    end_date_cume_dist,
    start_date_label,
    end_date_quintile_plus_rn_2,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown
FROM annotated
ORDER BY status, id
LIMIT 10
