WITH scoped AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        LENGTH(TRIM(region)) AS region_len,
        CASE WHEN region IN ('North', 'East') THEN 'group_a' WHEN region = 'West' THEN 'group_b' ELSE 'other' END AS region_group,
        CONCAT_WS('-', region, segment) AS region_segment_label
    FROM policyholders
    WHERE UPPER(TRIM(holder_name)) LIKE 'M%'
      AND region = 'North'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        region,
        risk_score,
        segment,
        region_len,
        region_group,
        region_segment_label,
        risk_score - LAG(risk_score) OVER w_region_region_group_by_risk AS risk_score_change,
        NTILE(10) OVER w_region_region_group_by_risk AS risk_score_decile,
        ROUND(risk_score / NULLIF(SUM(risk_score) OVER (PARTITION BY region_len, region_group), 0), 4) AS risk_score_share_of_region_len_region_group,
        COUNT(*) OVER (PARTITION BY region_len, region_group) AS rows_in_region_len_region_group,
        region_len - LAG(region_len) OVER w_region_len_by_risk_score AS region_len_change,
        RANK() OVER (PARTITION BY region, region_group ORDER BY risk_score DESC) AS risk_score_rank
    FROM scoped
    WINDOW w_region_region_group_by_risk AS (PARTITION BY region, region_group ORDER BY risk_score DESC, id), w_region_len_by_risk_score AS (PARTITION BY region_len ORDER BY risk_score DESC, id)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        region,
        risk_score,
        segment,
        region_len,
        region_group,
        region_segment_label,
        risk_score_change,
        risk_score_decile,
        risk_score_share_of_region_len_region_group,
        rows_in_region_len_region_group,
        region_len_change
    FROM windowed
    WHERE risk_score_rank <= 2
      AND rows_in_region_len_region_group <= 17
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY rows_in_region_len_region_group, id), 3) AS rows_in_region_len_region_group_cume_dist,
        ROUND(region_len_change - AVG(region_len_change) OVER (PARTITION BY region, region_group), 2) AS region_len_change_vs_region_region_group_avg,
        DENSE_RANK() OVER (ORDER BY rows_in_region_len_region_group) AS rows_in_region_len_region_group_dense_rank
    FROM leaders
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(risk_score_decile, region_len) AS least_risk_score_decile_region_len,
        LENGTH(TRIM(region_segment_label)) AS region_segment_label_len,
        SUBSTRING(TRIM(region_group), 1, 4) AS region_group_prefix
    FROM metrics
),
with_activity AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.status IN ('LAPSED', 'EXPIRED')
        )
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        region,
        risk_score,
        segment,
        region_group,
        region_segment_label,
        risk_score_change,
        risk_score_decile,
        rows_in_region_len_region_group,
        region_len_change,
        rows_in_region_len_region_group_cume_dist,
        rows_in_region_len_region_group_dense_rank,
        least_risk_score_decile_region_len,
        region_segment_label_len,
        region_group_prefix,
        region_len_change - LAG(region_len_change) OVER w_region_segment_label_len_by AS region_len_change_change,
        LAG(risk_score, 1, 0) OVER (ORDER BY region_len_change_vs_region_region_group_avg ASC, id) AS prev_risk_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY region_segment_label_len ORDER BY rows_in_region_len_region_group_cume_dist DESC), 3) AS rows_in_region_len_region_group_cume_dist_percen,
        ROUND(region_len_change - AVG(region_len_change) OVER w_region_len_risk_score_decile, 2) AS region_len_change_vs_region_len_risk_score_decil,
        LEAD(region_segment_label_len, 3) OVER w_region_segment_label_len_by AS next3_region_segment_label_len,
        LEAD(region_segment_label_len) OVER (PARTITION BY region_segment_label_len ORDER BY rows_in_region_len_region_group_cume_dist DESC, id) AS next_region_segment_label_len
    FROM with_activity
    WINDOW w_region_segment_label_len_by AS (PARTITION BY region_segment_label_len ORDER BY rows_in_region_len_region_group_cume_dist DESC, id), w_region_len_risk_score_decile AS (PARTITION BY region_len, risk_score_decile)
)
-- Final projection
SELECT
    id,
    region,
    risk_score,
    segment,
    region_group,
    region_segment_label,
    risk_score_change,
    risk_score_decile,
    rows_in_region_len_region_group,
    region_len_change,
    rows_in_region_len_region_group_cume_dist,
    region_segment_label_len,
    region_group_prefix,
    region_len_change_change,
    prev_risk_score,
    rows_in_region_len_region_group_cume_dist_percen,
    region_len_change_vs_region_len_risk_score_decil,
    next3_region_segment_label_len,
    CASE WHEN region_group IN ('other', 'group_a') THEN 'group_a' WHEN region_group = 'group_b' THEN 'group_b' ELSE 'other' END AS region_group_group,
    LEAST(risk_score, risk_score_decile) AS least_risk_score_risk_score_decile
FROM sequenced
ORDER BY risk_score_change
