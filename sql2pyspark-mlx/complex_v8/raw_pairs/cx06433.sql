WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE product IN ('TRAVEL', 'AUTO', 'LIFE')
      AND start_date >= DATE '2024-11-20'
),
screened AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM valid_policys s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium >= 1132.1
              AND YEAR(p.end_date) = 2026
        )
      AND s.start_date >= DATE '2024-10-22'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(premium) OVER (PARTITION BY holder_id ORDER BY start_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER w_start_date_month, 0), 4) AS premium_share_of_start_date_month,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date)) AS rows_in_start_date_month
    FROM screened
    WINDOW w_start_date_month AS (PARTITION BY DATE_TRUNC('MONTH', start_date))
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(rows_in_start_date_month) OVER (PARTITION BY YEAR(end_date) ORDER BY premium_share_of_start_date_month ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_rows_in_start_date_month_so_far,
        ROW_NUMBER() OVER (PARTITION BY YEAR(end_date) ORDER BY premium_share_of_start_date_month ASC, id) AS premium_share_of_start_date_month_seq,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY YEAR(start_date), holder_id ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        ROUND(AVG(rows_in_start_date_month) OVER (PARTITION BY holder_id), 2) AS holder_id_avg_rows_in_start_date_month,
        DENSE_RANK() OVER (PARTITION BY YEAR(end_date) ORDER BY premium_share_of_start_date_month ASC) AS premium_share_of_start_date_month_dense_rank
    FROM ranked
),
enriched AS (
    -- Derive helper columns
    SELECT *, MONTH(end_date) AS end_date_month
    FROM sequenced
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.running_premium,
        s.premium_share_of_start_date_month,
        s.rows_in_start_date_month,
        s.holder_id_avg_rows_in_start_date_month,
        s.premium_share_of_start_date_month_dense_rank,
        s.end_date_month,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region,
        p.holder_name
    FROM enriched s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    id,
    holder_id,
    premium,
    start_date,
    end_date,
    running_premium,
    premium_share_of_start_date_month,
    rows_in_start_date_month,
    holder_id_avg_rows_in_start_date_month,
    premium_share_of_start_date_month_dense_rank,
    end_date_month,
    holder_risk_score,
    holder_region,
    holder_name,
    LENGTH(TRIM(holder_name)) AS holder_name_len
FROM with_parent
ORDER BY running_premium NULLS LAST
