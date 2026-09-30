WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        segment,
        LAST_DAY(joined_on) AS joined_on_month_end
    FROM policyholders
    WHERE (segment IN ('RETAIL', 'VIP') OR UPPER(TRIM(holder_name)) LIKE 'E%')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        risk_score,
        joined_on,
        segment,
        SUM(risk_score) OVER w_segment_joined_on_month AS segment_joined_on_month_sum_risk_score,
        NTILE(4) OVER w_joined_on_month_end_month_by AS joined_on_quartile,
        LAG(joined_on_month_end) OVER (PARTITION BY DATE_TRUNC('MONTH', joined_on_month_end) ORDER BY joined_on ASC, id) AS prev_joined_on_month_end
    FROM prepared
    WINDOW w_segment_joined_on_month AS (PARTITION BY segment, DATE_TRUNC('MONTH', joined_on)), w_joined_on_month_end_month_by AS (PARTITION BY DATE_TRUNC('MONTH', joined_on_month_end) ORDER BY joined_on ASC, id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(joined_on) AS joined_on_year,
        DATE_FORMAT(prev_joined_on_month_end, 'yyyy-MM') AS prev_joined_on_month_end_ym,
        ROUND(AVG(risk_score), 2) AS avg_risk_score,
        MAX(prev_joined_on_month_end) AS last_prev_joined_on_month_end,
        ROUND(100 * COUNT(CASE WHEN segment IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_segment_is_not_null,
        COUNT(*) FILTER (WHERE segment = 'SME') AS n_segment_sme,
        SUM(risk_score) FILTER (WHERE segment = 'SME') AS risk_score_segment_sme
    FROM with_trends
    WHERE prev_joined_on_month_end IS NOT NULL
    GROUP BY YEAR(joined_on), DATE_FORMAT(prev_joined_on_month_end, 'yyyy-MM')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        risk_score_segment_sme - LAG(risk_score_segment_sme) OVER (PARTITION BY joined_on_year, n_segment_sme ORDER BY risk_score_segment_sme DESC, joined_on_year, prev_joined_on_month_end_ym) AS risk_score_segment_sme_change,
        LAG(avg_risk_score) OVER (PARTITION BY joined_on_year, n_segment_sme ORDER BY risk_score_segment_sme DESC, joined_on_year, prev_joined_on_month_end_ym) AS prev_avg_risk_score,
        ROW_NUMBER() OVER w_joined_on_year_by_avg_risk_s AS seq_no
    FROM totals
    WINDOW w_joined_on_year_by_avg_risk_s AS (PARTITION BY joined_on_year ORDER BY avg_risk_score, joined_on_year, prev_joined_on_month_end_ym)
)
-- Subtotals with GROUPING SETS
SELECT
    n_segment_sme,
    seq_no,
    GROUPING_ID() AS gid,
    MIN(n_segment_sme) AS min_n_segment_sme,
    COUNT(*) AS n_rows
FROM sequenced
GROUP BY GROUPING SETS ((n_segment_sme, seq_no), (n_segment_sme), ())
ORDER BY n_segment_sme NULLS LAST, seq_no NULLS LAST
