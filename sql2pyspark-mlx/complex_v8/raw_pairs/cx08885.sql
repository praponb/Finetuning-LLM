WITH filtered AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        DATE_FORMAT(joined_on, 'yyyy-MM-dd') AS joined_on_label,
        DATEDIFF(DATE '2026-01-01', joined_on) AS days_since_joined_on
    FROM policyholders
    WHERE region = 'South'
      AND UPPER(TRIM(holder_name)) LIKE 'L%'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY days_since_joined_on ASC, id) AS days_since_joined_on_rank,
        ROUND(AVG(days_since_joined_on) OVER w_joined_on_year, 2) AS joined_on_year_avg_days_since_joined_on,
        MAX(days_since_joined_on) OVER (PARTITION BY YEAR(joined_on)) AS joined_on_year_max_days_since_joined_on,
        COUNT(*) OVER (PARTITION BY YEAR(joined_on)) AS rows_in_joined_on_year,
        MAX(risk_score) OVER (PARTITION BY YEAR(joined_on)) AS joined_on_year_max_risk_score,
        NTILE(10) OVER w_all_by_joined_on AS joined_on_decile,
        ROUND(AVG(risk_score) OVER (ORDER BY days_since_joined_on ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS risk_score_avg_6_prior,
        ROW_NUMBER() OVER (ORDER BY risk_score, id) AS seq_no
    FROM filtered
    WINDOW w_joined_on_year AS (PARTITION BY YEAR(joined_on)), w_all_by_joined_on AS (ORDER BY joined_on, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        joined_on_label,
        days_since_joined_on,
        days_since_joined_on_rank,
        joined_on_year_avg_days_since_joined_on,
        joined_on_year_max_days_since_joined_on,
        COUNT(*) OVER w_days_since_joined_on_rank AS rows_in_days_since_joined_on_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY seq_no ORDER BY days_since_joined_on_rank, id), 3) AS days_since_joined_on_rank_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY joined_on_year_max_risk_score ASC), 3) AS joined_on_year_max_risk_score_percent_rank,
        DENSE_RANK() OVER (ORDER BY joined_on_year_max_risk_score ASC) AS joined_on_year_max_risk_score_dense_rank,
        DENSE_RANK() OVER (PARTITION BY seq_no ORDER BY days_since_joined_on_rank, id) AS days_since_joined_on_rank_dense_rank,
        ROUND(joined_on_decile / NULLIF(SUM(joined_on_decile) OVER (PARTITION BY days_since_joined_on_rank), 0), 4) AS joined_on_decile_share_of_days_since_joined_on_r
    FROM ranked
    WINDOW w_days_since_joined_on_rank AS (PARTITION BY days_since_joined_on_rank)
)
SELECT
    id,
    risk_score,
    joined_on,
    joined_on_label,
    days_since_joined_on,
    joined_on_year_max_days_since_joined_on,
    rows_in_days_since_joined_on_rank,
    days_since_joined_on_rank_cume_dist,
    joined_on_year_max_risk_score_percent_rank,
    days_since_joined_on_rank_dense_rank,
    DATE_TRUNC('MONTH', joined_on) AS joined_on_month,
    days_since_joined_on - risk_score AS days_since_joined_on_minus_risk_score
FROM with_trends
WHERE days_since_joined_on_rank < 6
ORDER BY rows_in_days_since_joined_on_rank DESC NULLS FIRST, joined_on_year_max_days_since_joined_on
