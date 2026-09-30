WITH cleaned_holders AS (
    -- Filter policyholders
    SELECT
        id,
        region,
        risk_score,
        segment,
        DATE_FORMAT(joined_on, 'MMM yyyy') AS joined_on_label
    FROM policyholders
    WHERE LENGTH(TRIM(holder_name)) >= 7
      AND region <> 'West'
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.premium) FROM policies p WHERE p.holder_id = s.id) AS max_policy_premium,
        (SELECT MIN(p.start_date) FROM policies p WHERE p.holder_id = s.id) AS first_policy_start_date
    FROM cleaned_holders s
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        segment,
        first_policy_start_date,
        MIN(max_policy_premium) OVER (PARTITION BY segment) AS segment_min_max_policy_premium,
        LAG(max_policy_premium) OVER (ORDER BY first_policy_start_date NULLS LAST, id) AS prev_max_policy_premium,
        COUNT(*) OVER (PARTITION BY segment) AS rows_in_segment,
        ROUND(risk_score - AVG(risk_score) OVER (PARTITION BY segment), 2) AS risk_score_vs_segment_avg,
        ROUND(AVG(max_policy_premium) OVER (ORDER BY risk_score, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS max_policy_premium_avg_6_prior
    FROM summarized_rows
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', first_policy_start_date) AS first_policy_start_date_quarter
    FROM windowed
    WHERE first_policy_start_date < DATE '2025-07-01'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT first_policy_start_date_quarter, first_policy_start_date, segment, segment_min_max_policy_premium
    FROM labeled
) src
PIVOT (
    AVG(segment_min_max_policy_premium)
    FOR segment IN ('RETAIL', 'SME', 'VIP')
)
ORDER BY first_policy_start_date_quarter, first_policy_start_date
