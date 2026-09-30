WITH filtered AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        COALESCE(rate, 0.0) AS rate_filled,
        CASE WHEN rate >= 5.5 THEN 'A' WHEN rate >= 4.2 THEN 'B' ELSE 'C' END AS rate_tier
    FROM tariff_windows
    WHERE rate >= 3.0
      AND TO_DATE(valid_from) < DATE '2025-03-09'
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(valid_to, 'yyyy-MM') AS valid_to_label,
        CONCAT(rate_tier, '#', CAST(id AS STRING)) AS rate_tier_ref,
        DATE_FORMAT(valid_to, 'yyyy-MM') AS valid_to_label_2
    FROM filtered
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        rate_tier,
        YEAR(valid_to) AS valid_to_year,
        MIN(valid_to) AS first_valid_to,
        ROUND(SUM(rate), 2) AS total_rate
    FROM annotated
    WHERE HOUR(valid_to) BETWEEN 9 AND 15
    GROUP BY 1, 2
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(total_rate, 2) OVER (PARTITION BY rate_tier ORDER BY first_valid_to, rate_tier, valid_to_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_rate,
        SUM(valid_to_year) OVER (PARTITION BY rate_tier ORDER BY first_valid_to, rate_tier, valid_to_year ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS valid_to_year_sum_3_prior,
        SUM(total_rate) OVER (PARTITION BY rate_tier ORDER BY first_valid_to, rate_tier, valid_to_year ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS total_rate_sum_1_prior
    FROM rollup_base
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(total_rate_sum_1_prior, second_total_rate) AS least_total_rate_sum_1_prior_second_total_rate
    FROM with_trends
)
-- Final projection
SELECT
    rate_tier,
    valid_to_year,
    first_valid_to,
    total_rate,
    valid_to_year_sum_3_prior,
    total_rate_sum_1_prior,
    least_total_rate_sum_1_prior_second_total_rate,
    DATE_TRUNC('DAY', first_valid_to) AS first_valid_to_day
FROM scored
ORDER BY total_rate_sum_1_prior, rate_tier, valid_to_year
LIMIT 20
