WITH prepared_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        risk_score,
        segment,
        LOWER(REGEXP_REPLACE(TRIM(region), ' ', '_')) AS region_slug
    FROM policyholders
    WHERE joined_on >= DATE '2024-07-18'
      AND region IN ('South', 'North', 'East')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_name,
        risk_score,
        segment,
        region_slug,
        LAG(risk_score) OVER (ORDER BY risk_score NULLS LAST, id) AS prev_risk_score,
        ROUND(AVG(risk_score) OVER (ORDER BY risk_score, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS risk_score_avg_2,
        MAX(risk_score) OVER (PARTITION BY segment) AS segment_max_risk_score,
        ROUND(risk_score / NULLIF(SUM(risk_score) OVER (PARTITION BY segment), 0), 4) AS risk_score_share_of_segment,
        risk_score - LAG(risk_score) OVER (PARTITION BY segment ORDER BY risk_score, id) AS risk_score_change,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY risk_score, id) AS seq_no,
        SUM(risk_score) OVER w_segment AS segment_sum_risk_score,
        ROW_NUMBER() OVER (ORDER BY risk_score, id) AS risk_score_seq,
        risk_score - LAG(risk_score) OVER (ORDER BY risk_score NULLS LAST, id) AS risk_score_change_2,
        COUNT(*) OVER w_segment AS rows_in_segment,
        NTH_VALUE(risk_score, 2) OVER (ORDER BY risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_risk_score,
        NTILE(4) OVER (PARTITION BY segment ORDER BY risk_score, id) AS risk_score_quartile,
        MIN(risk_score) OVER (PARTITION BY segment) AS segment_min_risk_score,
        ROUND(risk_score - AVG(risk_score) OVER (PARTITION BY segment), 2) AS risk_score_vs_segment_avg
    FROM prepared_holders
    WINDOW w_segment AS (PARTITION BY segment)
),
summary AS (
    -- Group-level totals
    SELECT
        seq_no,
        risk_score_seq,
        SUM(risk_score_change) FILTER (WHERE segment = 'VIP') AS risk_score_change_segment_vip,
        ROUND(AVG(rows_in_segment), 2) AS avg_rows_in_segment,
        ROUND(100 * COUNT(CASE WHEN segment NOT IN ('VIP', 'RETAIL') THEN 1 END) / COUNT(*), 1) AS pct_segment_not_in_vip_retail,
        ARRAY_SORT(COLLECT_SET(holder_name)) AS holder_name_list
    FROM with_trends
    WHERE risk_score_change_2 <= 0.5
    GROUP BY seq_no, risk_score_seq
    HAVING AVG(rows_in_segment) > 8
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY risk_score_seq ORDER BY avg_rows_in_segment DESC NULLS FIRST, seq_no, risk_score_seq) AS seq_no_2,
        RANK() OVER (PARTITION BY seq_no ORDER BY pct_segment_not_in_vip_retail) AS pct_segment_not_in_vip_retail_rank,
        ROUND(avg_rows_in_segment - AVG(avg_rows_in_segment) OVER (PARTITION BY risk_score_seq, seq_no), 2) AS avg_rows_in_segment_vs_risk_score_seq_seq_no_avg
    FROM summary
)
SELECT
    seq_no,
    risk_score_seq,
    risk_score_change_segment_vip,
    pct_segment_not_in_vip_retail,
    holder_name_list,
    seq_no_2,
    pct_segment_not_in_vip_retail_rank,
    CASE WHEN risk_score_seq >= 7 THEN 'large' WHEN risk_score_seq >= 5 THEN 'medium' ELSE 'small' END AS risk_score_seq_tier
FROM ranked
ORDER BY seq_no_2 DESC, pct_segment_not_in_vip_retail_rank DESC NULLS FIRST
