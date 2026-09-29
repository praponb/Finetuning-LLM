WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        segment,
        CASE WHEN segment IN ('VIP', 'SME') THEN 'group_a' WHEN segment = 'RETAIL' THEN 'group_b' ELSE 'other' END AS segment_group,
        COALESCE(risk_score, 0.0) AS risk_score_filled
    FROM policyholders
    WHERE risk_score <= 0.6
      AND UPPER(TRIM(holder_name)) LIKE 'N%'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(risk_score_filled) OVER (ORDER BY risk_score ASC NULLS LAST, id) AS next_risk_score_filled,
        FIRST_VALUE(risk_score) IGNORE NULLS OVER (PARTITION BY segment_group, segment ORDER BY risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_risk_score,
        MAX(risk_score) OVER (PARTITION BY segment_group, segment) AS segment_group_segment_max_risk_score,
        ROUND(PERCENT_RANK() OVER w_joined_on_year_by_risk_score, 3) AS risk_score_filled_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY risk_score ASC NULLS LAST), 3) AS risk_score_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY segment_group, segment ORDER BY risk_score ASC, id) AS rn,
        NTILE(4) OVER (PARTITION BY segment_group, segment ORDER BY risk_score ASC, id) AS risk_score_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY segment, segment_group ORDER BY risk_score_filled DESC, id), 3) AS risk_score_filled_percent_rank_2,
        NTILE(4) OVER (PARTITION BY YEAR(joined_on) ORDER BY risk_score_filled DESC NULLS FIRST, id) AS risk_score_filled_quartile
    FROM prepared
    WINDOW w_joined_on_year_by_risk_score AS (PARTITION BY YEAR(joined_on) ORDER BY risk_score_filled DESC NULLS FIRST)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        segment,
        segment_group,
        risk_score_filled,
        next_risk_score_filled,
        first_known_risk_score,
        segment_group_segment_max_risk_score,
        risk_score_filled_percent_rank,
        risk_score_percent_rank,
        risk_score_quartile,
        risk_score_filled_percent_rank_2,
        risk_score_filled_quartile
    FROM with_trends
    WHERE rn <= 5
      AND risk_score_quartile >= 3
)
SELECT
    id,
    holder_name,
    joined_on,
    segment,
    next_risk_score_filled,
    first_known_risk_score,
    segment_group_segment_max_risk_score,
    risk_score_filled_percent_rank,
    risk_score_percent_rank,
    risk_score_quartile,
    risk_score_filled_quartile,
    DATEDIFF(DATE '2026-01-01', joined_on) AS days_since_joined_on
FROM first_rows
WHERE risk_score_quartile <= 8
