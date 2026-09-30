WITH filtered AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        ABS(rate - 4.2) AS rate_distance,
        DATEDIFF(DATE '2026-01-01', valid_to) AS days_since_valid_to
    FROM tariff_windows
    WHERE tariff IN ('TOU', 'FLAT')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY TO_DATE(valid_from), tariff ORDER BY rate, id) AS rate_quartile,
        MAX(rate) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from), tariff ORDER BY rate_distance ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rate_max_6_prior,
        MAX(rate) OVER w_tariff AS tariff_max_rate,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(valid_from), tariff ORDER BY rate, id) AS seq_no,
        RANK() OVER w_valid_from_month_tariff_by_r_2 AS rate_distance_rank
    FROM filtered
    WINDOW w_tariff AS (PARTITION BY tariff), w_valid_from_to_date_tariff_by AS (PARTITION BY TO_DATE(valid_from), tariff ORDER BY rate, id), w_valid_from_month_tariff_by_r_2 AS (PARTITION BY DATE_TRUNC('MONTH', valid_from), tariff ORDER BY rate_distance ASC)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(days_since_valid_to) OVER (PARTITION BY rate_quartile) AS rate_quartile_min_days_since_valid_to,
        SIZE(COLLECT_SET(tariff) OVER (PARTITION BY rate_quartile)) AS distinct_tariff_in_rate_quartile,
        LEAD(valid_from, 3) OVER (PARTITION BY tariff, rate_distance_rank ORDER BY valid_from, id) AS next3_valid_from,
        LAG(rate_quartile) OVER w_tariff_rate_distance_rank_by AS prev_rate_quartile,
        ROUND(rate_max_6_prior - AVG(rate_max_6_prior) OVER w_rate_quartile, 2) AS rate_max_6_prior_vs_rate_quartile_avg,
        ROUND(days_since_valid_to / NULLIF(SUM(days_since_valid_to) OVER (PARTITION BY rate_quartile), 0), 4) AS days_since_valid_to_share_of_rate_quartile
    FROM ranked
    WINDOW w_tariff_rate_distance_rank_by AS (PARTITION BY tariff, rate_distance_rank ORDER BY valid_from, id), w_rate_quartile AS (PARTITION BY rate_quartile)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(tariff_max_rate / NULLIF(seq_no, 0), 3) AS tariff_max_rate_per_seq_no,
        SUBSTRING(TRIM(tariff), 1, 4) AS tariff_prefix,
        LENGTH(TRIM(tariff)) AS tariff_len
    FROM metrics
),
latest_per_key AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        rate_distance,
        days_since_valid_to,
        rate_quartile,
        rate_max_6_prior,
        tariff_max_rate,
        seq_no,
        rate_distance_rank,
        rate_quartile_min_days_since_valid_to,
        distinct_tariff_in_rate_quartile,
        next3_valid_from,
        prev_rate_quartile,
        rate_max_6_prior_vs_rate_quartile_avg,
        days_since_valid_to_share_of_rate_quartile,
        tariff_max_rate_per_seq_no,
        tariff_prefix,
        tariff_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY rate_quartile DESC, id DESC) AS recency_rank
        FROM annotated
    ) ranked
    WHERE recency_rank = 1
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(rate / NULLIF(tariff_len, 0), 3) AS rate_per_tariff_len,
        ROUND(seq_no / NULLIF(rate_distance, 0), 3) AS seq_no_per_rate_distance,
        CASE WHEN tariff IN ('EV', 'TOU') THEN 'group_a' WHEN tariff = 'FLAT' THEN 'group_b' ELSE 'other' END AS tariff_group
    FROM latest_per_key
)
-- Final projection
SELECT
    id,
    valid_from,
    rate,
    rate_distance,
    days_since_valid_to,
    rate_max_6_prior,
    rate_distance_rank,
    rate_quartile_min_days_since_valid_to,
    next3_valid_from,
    rate_max_6_prior_vs_rate_quartile_avg,
    days_since_valid_to_share_of_rate_quartile,
    tariff_max_rate_per_seq_no,
    seq_no_per_rate_distance,
    tariff_group,
    DATEDIFF(DATE '2025-06-30', next3_valid_from) AS days_since_next3_valid_from,
    ROUND((UNIX_TIMESTAMP(next3_valid_from) - UNIX_TIMESTAMP(valid_from)) / 60, 1) AS minutes_valid_from_to_next3_valid_from
FROM enriched
WHERE days_since_valid_to_share_of_rate_quartile < 0.7
ORDER BY rate_distance DESC
