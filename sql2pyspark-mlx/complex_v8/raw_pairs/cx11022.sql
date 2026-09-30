WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        CASE WHEN rate >= 5.7 THEN 'HIGH' WHEN rate >= 4.2 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier,
        CASE WHEN tariff IN ('EV', 'FLAT') THEN 'group_a' WHEN tariff = 'TOU' THEN 'group_b' ELSE 'other' END AS tariff_group
    FROM tariff_windows
    WHERE (tariff NOT IN ('FLAT', 'EV') OR TO_DATE(valid_to) < DATE '2025-03-09')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        valid_from,
        rate,
        rate_tier,
        tariff_group,
        FIRST_VALUE(rate) OVER (PARTITION BY tariff_group ORDER BY valid_to, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rate,
        ROUND(AVG(rate) OVER (PARTITION BY tariff_group ORDER BY valid_to, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS rate_avg_2_prior,
        ROUND(rate - AVG(rate) OVER (PARTITION BY tariff_group, rate_tier), 2) AS rate_vs_tariff_group_rate_tier_avg,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY tariff_group, rate_tier), 0), 4) AS rate_share_of_tariff_group_rate_tier,
        LEAD(rate, 2) OVER w_tariff_group_by_valid_to AS next2_rate
    FROM prepared
    WINDOW w_tariff_group_by_valid_to AS (PARTITION BY tariff_group ORDER BY valid_to, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(tariff_group), ' ', '_')) AS tariff_group_slug,
        DATE_TRUNC('MONTH', valid_from) AS valid_from_month,
        YEAR(valid_from) AS valid_from_year
    FROM windowed
),
windowed_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(next2_rate) OVER (PARTITION BY valid_from_year) AS valid_from_year_sum_next2_rate,
        ROUND(CUME_DIST() OVER (PARTITION BY valid_from_year, TO_DATE(valid_from) ORDER BY valid_from DESC, id), 3) AS valid_from_cume_dist,
        LAST_VALUE(rate) OVER (PARTITION BY valid_from_year, TO_DATE(valid_from) ORDER BY valid_from DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rate
    FROM derived
)
SELECT DISTINCT rate_share_of_tariff_group_rate_tier
FROM windowed_2
WHERE rate_tier IS NOT NULL
ORDER BY rate_share_of_tariff_group_rate_tier DESC
