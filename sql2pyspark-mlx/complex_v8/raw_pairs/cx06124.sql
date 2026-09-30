WITH cleaned AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        SUBSTRING(TRIM(tariff), 1, 4) AS tariff_prefix,
        DATEDIFF(DATE '2025-12-31', valid_from) AS days_since_valid_from
    FROM tariff_windows
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(rate) OVER (PARTITION BY tariff ORDER BY valid_from, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS rate_avg_4,
        MIN(days_since_valid_from) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from)) AS valid_from_month_min_days_since_valid_from,
        FIRST_VALUE(valid_to) IGNORE NULLS OVER (PARTITION BY tariff ORDER BY valid_from DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_valid_to,
        FIRST_VALUE(rate) IGNORE NULLS OVER (PARTITION BY tariff ORDER BY valid_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rate,
        MAX(rate) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from)) AS valid_from_month_max_rate
    FROM cleaned
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM with_trends
    WHERE valid_from_month_min_days_since_valid_from > (SELECT MIN(valid_from_month_min_days_since_valid_from) FROM with_trends)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        days_since_valid_from - LAG(days_since_valid_from) OVER (PARTITION BY tariff ORDER BY rate_avg_4 ASC, id) AS days_since_valid_from_change,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(LAG(valid_from) OVER (PARTITION BY tariff ORDER BY rate_avg_4 ASC, id))) / 60, 1) AS minutes_since_prev_valid_from,
        COUNT(*) OVER (PARTITION BY tariff ORDER BY rate_avg_4 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(valid_to) OVER (PARTITION BY tariff ORDER BY rate_avg_4 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_valid_to,
        MIN(valid_from_month_max_rate) OVER (PARTITION BY tariff ORDER BY rate_avg_4 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_valid_from_month_max_rate_so_far
    FROM filtered
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        tariff_ref,
        tariff_prefix,
        days_since_valid_from,
        rate_avg_4,
        valid_from_month_min_days_since_valid_from,
        first_known_valid_to,
        first_known_rate,
        days_since_valid_from_change,
        minutes_since_prev_valid_from,
        running_count,
        TO_DATE(last_valid_to) AS last_valid_to_date,
        LENGTH(TRIM(tariff_ref)) AS tariff_ref_len,
        CASE WHEN running_count >= 14 THEN 'large' WHEN running_count >= 6 THEN 'medium' ELSE 'small' END AS running_count_tier
    FROM windowed
),
running_count_tier_stats AS (
    -- Avg rate_avg_4 per running_count_tier
    SELECT running_count_tier, AVG(rate_avg_4) AS avg_rate_avg_4
    FROM scored
    GROUP BY running_count_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_rate_avg_4,
        ROUND(s.rate_avg_4 - g.avg_rate_avg_4, 2) AS rate_avg_4_minus_avg
    FROM scored s
    JOIN running_count_tier_stats g ON g.running_count_tier = s.running_count_tier
    WHERE s.rate_avg_4 > g.avg_rate_avg_4
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM compared
    WHERE rate > (SELECT AVG(rate) FROM compared)
)
SELECT
    id,
    valid_from,
    valid_to,
    rate,
    tariff_ref,
    days_since_valid_from,
    rate_avg_4,
    valid_from_month_min_days_since_valid_from,
    first_known_valid_to,
    first_known_rate,
    days_since_valid_from_change,
    running_count,
    tariff_ref_len,
    CONCAT(tariff_ref, '#', CAST(id AS STRING)) AS tariff_ref_ref
FROM screened
WHERE rate_avg_4 BETWEEN 3.8 AND 6.2
ORDER BY tariff_ref_ref
