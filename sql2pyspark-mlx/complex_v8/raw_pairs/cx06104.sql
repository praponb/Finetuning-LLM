WITH filtered AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        DATE_FORMAT(joined_on, 'MMM yyyy') AS joined_on_label
    FROM policyholders
    WHERE (risk_score > 0.1 OR UPPER(TRIM(holder_name)) LIKE 'D%')
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', joined_on) AS joined_on_month,
        ARRAY_SORT(COLLECT_SET(holder_name)) AS holder_name_list,
        MIN(joined_on) AS first_joined_on,
        SUM(risk_score) AS total_risk_score,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(risk_score, 0.5) AS median_risk_score
    FROM filtered
    WHERE joined_on_label IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', joined_on)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        joined_on_month,
        holder_name_list,
        first_joined_on,
        total_risk_score,
        row_count,
        SUM(median_risk_score) OVER w_joined_on_month_month AS joined_on_month_month_sum_median_risk_score,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', joined_on_month)) AS rows_in_joined_on_month_month,
        FIRST_VALUE(joined_on_month) IGNORE NULLS OVER (PARTITION BY YEAR(first_joined_on) ORDER BY median_risk_score DESC, joined_on_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_joined_on_month
    FROM summary
    WINDOW w_joined_on_month_month AS (PARTITION BY DATE_TRUNC('MONTH', joined_on_month))
),
metrics AS (
    -- Previous / next values per key
    SELECT
        joined_on_month,
        first_joined_on,
        total_risk_score,
        joined_on_month_month_sum_median_risk_score,
        rows_in_joined_on_month_month,
        first_known_joined_on_month,
        ROW_NUMBER() OVER w_first_known_joined_on_month AS seq_no,
        MAX(total_risk_score) OVER (PARTITION BY TO_DATE(first_known_joined_on_month)) AS first_known_joined_on_month_to_date_max_total_ri,
        ROUND(total_risk_score / NULLIF(SUM(total_risk_score) OVER (PARTITION BY TO_DATE(first_known_joined_on_month)), 0), 4) AS total_risk_score_share_of_first_known_joined_on_,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(first_known_joined_on_month), row_count ORDER BY rows_in_joined_on_month_month NULLS LAST, joined_on_month) AS rows_in_joined_on_month_month_dense_rank
    FROM windowed
    WINDOW w_first_known_joined_on_month AS (PARTITION BY TO_DATE(first_known_joined_on_month), row_count ORDER BY rows_in_joined_on_month_month NULLS LAST, joined_on_month)
)
-- Final projection
SELECT
    joined_on_month,
    first_joined_on,
    total_risk_score,
    joined_on_month_month_sum_median_risk_score,
    rows_in_joined_on_month_month,
    first_known_joined_on_month,
    seq_no,
    first_known_joined_on_month_to_date_max_total_ri,
    total_risk_score_share_of_first_known_joined_on_,
    rows_in_joined_on_month_month_dense_rank,
    ROUND(total_risk_score * 0.9, 2) AS total_risk_score_adj
FROM metrics
