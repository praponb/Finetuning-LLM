WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE segment <> 'VIP'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(risk_score - AVG(risk_score) OVER (PARTITION BY segment), 2) AS risk_score_vs_segment_avg,
        MAX(risk_score) OVER (ORDER BY risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_risk_score_so_far,
        DATEDIFF(joined_on, LAG(joined_on) OVER w_all_by_risk_score) AS days_since_prev_joined_on,
        ROUND(risk_score / NULLIF(SUM(risk_score) OVER (PARTITION BY segment), 0), 4) AS risk_score_share_of_segment,
        MAX(risk_score) OVER (PARTITION BY segment ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_risk_score_so_far_2,
        MAX(risk_score) OVER w_segment AS segment_max_risk_score,
        FIRST_VALUE(joined_on) IGNORE NULLS OVER (PARTITION BY segment ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_joined_on,
        NTH_VALUE(joined_on, 2) OVER (ORDER BY risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_joined_on,
        ROUND(AVG(risk_score) OVER (PARTITION BY segment), 2) AS segment_avg_risk_score,
        COUNT(*) OVER (PARTITION BY segment) AS rows_in_segment
    FROM scoped
    WINDOW w_all_by_risk_score AS (ORDER BY risk_score DESC, id), w_segment AS (PARTITION BY segment)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(max_risk_score_so_far, segment_max_risk_score) AS greatest_max_risk_score_so_far_segment_max_risk_,
        GREATEST(segment_avg_risk_score, segment_max_risk_score) AS greatest_segment_avg_risk_score_segment_max_risk,
        CONCAT(segment, '#', CAST(id AS STRING)) AS segment_ref
    FROM ranked
    WHERE risk_score_share_of_segment <= 0.6
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY segment) AS rows_in_segment_2,
        LAST_VALUE(max_risk_score_so_far) OVER (PARTITION BY MONTH(second_joined_on), segment ORDER BY greatest_max_risk_score_so_far_segment_max_risk_ DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_max_risk_score_so_far,
        COUNT(*) OVER w_second_joined_on_month AS rows_in_second_joined_on_month,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_joined_on) ORDER BY segment_avg_risk_score ASC, id) AS segment_avg_risk_score_seq,
        SUM(days_since_prev_joined_on) OVER (PARTITION BY segment) AS segment_sum_days_since_prev_joined_on
    FROM annotated
    WINDOW w_second_joined_on_month AS (PARTITION BY MONTH(second_joined_on))
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        segment,
        YEAR(joined_on) AS joined_on_year,
        SUM(CASE WHEN segment = 'SME' THEN 1 ELSE 0 END) AS n_segment_sme,
        COUNT(DISTINCT segment) AS distinct_segments,
        SUM(CASE WHEN segment <> 'SME' THEN 1 ELSE 0 END) AS n_segment_sme_2,
        COUNT(*) AS row_count,
        MIN(joined_on) AS first_joined_on
    FROM windowed
    WHERE risk_score BETWEEN 0.2 AND 0.9
    GROUP BY segment, YEAR(joined_on)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        segment,
        joined_on_year,
        n_segment_sme,
        distinct_segments,
        n_segment_sme_2,
        row_count,
        ROUND(AVG(row_count) OVER (PARTITION BY n_segment_sme, joined_on_year), 2) AS n_segment_sme_joined_on_year_avg_row_count,
        ROW_NUMBER() OVER (PARTITION BY n_segment_sme_2 ORDER BY n_segment_sme_2 NULLS LAST, segment, joined_on_year) AS n_segment_sme_2_seq,
        LAG(n_segment_sme) OVER (PARTITION BY n_segment_sme_2 ORDER BY n_segment_sme_2 NULLS LAST, segment, joined_on_year) AS prev_n_segment_sme
    FROM stats
)
SELECT
    segment,
    joined_on_year,
    distinct_segments,
    row_count,
    n_segment_sme_joined_on_year_avg_row_count,
    n_segment_sme_2_seq,
    prev_n_segment_sme,
    LENGTH(TRIM(segment)) AS segment_len,
    ABS(joined_on_year - 2025) AS joined_on_year_distance
FROM with_windows
WHERE prev_n_segment_sme <= 9
ORDER BY prev_n_segment_sme
