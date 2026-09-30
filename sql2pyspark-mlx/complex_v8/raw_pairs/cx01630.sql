WITH recent AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        COALESCE(risk_score, 0.0) AS risk_score_filled
    FROM policyholders
    WHERE segment <> 'RETAIL'
      AND YEAR(joined_on) = 2024
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND YEAR(p.end_date) = 2026
              AND p.premium <= 3719.7
        )
),
totals AS (
    -- Summarise per group
    SELECT
        region,
        DATE_FORMAT(joined_on, 'yyyy-MM') AS joined_on_ym,
        MAX(risk_score) AS max_risk_score,
        MIN(joined_on) AS first_joined_on,
        ROUND(SUM(risk_score_filled), 2) AS total_risk_score_filled,
        COUNT(*) AS row_count
    FROM screened
    GROUP BY 1, 2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY row_count DESC), 3) AS row_count_percent_rank,
        NTILE(3) OVER (PARTITION BY row_count ORDER BY row_count DESC, region, joined_on_ym) AS row_count_tercile,
        NTILE(3) OVER (PARTITION BY YEAR(first_joined_on), region ORDER BY row_count, region, joined_on_ym) AS row_count_tercile_2,
        FIRST_VALUE(max_risk_score) IGNORE NULLS OVER (ORDER BY row_count DESC, region, joined_on_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_risk_score,
        SUM(total_risk_score_filled) OVER (PARTITION BY row_count ORDER BY row_count DESC, region, joined_on_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_risk_score_filled
    FROM totals
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(first_joined_on) ORDER BY max_risk_score ASC, region, joined_on_ym), 3) AS max_risk_score_percent_rank,
        ROUND(AVG(row_count_tercile_2) OVER (PARTITION BY row_count), 2) AS row_count_avg_row_count_tercile_2,
        MAX(total_risk_score_filled) OVER (PARTITION BY MONTH(first_joined_on) ORDER BY max_risk_score ASC, region, joined_on_ym ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total_risk_score_filled_max_2,
        ROUND(row_count_percent_rank / NULLIF(SUM(row_count_percent_rank) OVER (PARTITION BY row_count), 0), 4) AS row_count_percent_rank_share_of_row_count,
        DENSE_RANK() OVER (ORDER BY row_count_tercile_2) AS row_count_tercile_2_dense_rank
    FROM sequenced
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        region,
        joined_on_ym,
        max_risk_score,
        first_joined_on,
        row_count,
        row_count_percent_rank,
        first_known_max_risk_score,
        row_count_tercile_2_dense_rank,
        ROUND(AVG(row_count_percent_rank) OVER (PARTITION BY row_count_tercile_2_dense_rank), 2) AS row_count_tercile_2_dense_rank_avg_row_count_per,
        FIRST_VALUE(row_count_tercile) OVER (PARTITION BY row_count_tercile_2_dense_rank ORDER BY row_count_tercile ASC NULLS LAST, region, joined_on_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_row_count_tercile,
        max_risk_score_percent_rank - LAG(max_risk_score_percent_rank) OVER (PARTITION BY row_count_tercile_2_dense_rank ORDER BY row_count_tercile ASC NULLS LAST, region, joined_on_ym) AS max_risk_score_percent_rank_change
    FROM with_windows
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM with_windows_2
    WHERE row_count_percent_rank < (SELECT MAX(row_count_percent_rank) FROM with_windows_2)
)
-- Final projection
SELECT
    region,
    joined_on_ym,
    max_risk_score,
    first_joined_on,
    row_count,
    row_count_percent_rank,
    first_known_max_risk_score,
    row_count_tercile_2_dense_rank,
    first_row_count_tercile,
    max_risk_score_percent_rank_change,
    LOWER(TRIM(region)) AS region_clean
FROM filtered
ORDER BY max_risk_score, first_row_count_tercile DESC NULLS FIRST, region, joined_on_ym
LIMIT 10
