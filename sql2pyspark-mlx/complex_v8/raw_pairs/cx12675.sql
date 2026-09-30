WITH recent AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        CASE WHEN tariff IN ('EV', 'FLAT') THEN 'group_a' WHEN tariff = 'TOU' THEN 'group_b' ELSE 'other' END AS tariff_group,
        CASE WHEN tariff IN ('FLAT', 'TOU') THEN 'group_a' WHEN tariff = 'EV' THEN 'group_b' ELSE 'other' END AS tariff_group_2
    FROM tariff_windows
    WHERE (valid_from < TIMESTAMP '2025-03-09 04:00:00' OR tariff NOT IN ('FLAT', 'EV'))
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent
    WHERE rate > (SELECT AVG(rate) FROM recent)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(rate - AVG(rate) OVER w_tariff_group, 2) AS rate_vs_tariff_group_avg,
        SIZE(COLLECT_SET(tariff_group_2) OVER (PARTITION BY tariff_group)) AS distinct_tariff_group_2_in_tariff_group,
        SIZE(COLLECT_SET(tariff) OVER (PARTITION BY tariff_group)) AS distinct_tariff_in_tariff_group
    FROM filtered
    WINDOW w_tariff_group AS (PARTITION BY tariff_group)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_rate AS rate_seq,
        MAX(rate_vs_tariff_group_avg) OVER (PARTITION BY distinct_tariff_in_tariff_group, tariff_group_2 ORDER BY rate_vs_tariff_group_avg DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS rate_vs_tariff_group_avg_max_1_prior,
        LAST_VALUE(valid_from) OVER (ORDER BY rate DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_valid_from,
        ROUND(AVG(rate) OVER (PARTITION BY tariff_group, tariff ORDER BY rate_vs_tariff_group_avg DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS rate_avg_2,
        ROUND(AVG(rate_vs_tariff_group_avg) OVER (PARTITION BY distinct_tariff_in_tariff_group, tariff_group_2 ORDER BY rate_vs_tariff_group_avg DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS rate_vs_tariff_group_avg_avg_6_prior,
        SUM(rate_vs_tariff_group_avg) OVER w_tariff_distinct_tariff_group AS tariff_distinct_tariff_group_2_in_tariff_group_s
    FROM ranked
    WINDOW w_all_by_rate AS (ORDER BY rate DESC, id), w_tariff_distinct_tariff_group AS (PARTITION BY tariff, distinct_tariff_group_2_in_tariff_group)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(rate_avg_2, rate_seq) AS greatest_rate_avg_2_rate_seq,
        valid_to + INTERVAL 7 DAYS AS valid_to_plus_7d
    FROM ranked_2
    WHERE tariff_group = 'other'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_distinct_tariff_group_2_in_t AS rate_vs_tariff_group_avg_avg_6_prior_rank,
        COUNT(*) OVER w_tariff_group_2 AS rows_in_tariff_group_2,
        LAST_VALUE(valid_from) OVER (PARTITION BY distinct_tariff_group_2_in_tariff_group ORDER BY rate_vs_tariff_group_avg_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_valid_from_2,
        LAST_VALUE(valid_from) IGNORE NULLS OVER (PARTITION BY tariff, rate_seq ORDER BY greatest_rate_avg_2_rate_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_valid_from
    FROM annotated
    WINDOW w_distinct_tariff_group_2_in_t AS (PARTITION BY distinct_tariff_group_2_in_tariff_group ORDER BY rate_vs_tariff_group_avg_avg_6_prior), w_tariff_group_2 AS (PARTITION BY tariff_group_2)
)
-- Final projection
SELECT
    id,
    valid_from,
    valid_to,
    rate,
    tariff_group,
    rate_vs_tariff_group_avg,
    distinct_tariff_group_2_in_tariff_group,
    distinct_tariff_in_tariff_group,
    rate_seq,
    last_valid_from,
    rate_avg_2,
    rate_vs_tariff_group_avg_avg_6_prior,
    tariff_distinct_tariff_group_2_in_tariff_group_s,
    greatest_rate_avg_2_rate_seq,
    valid_to_plus_7d,
    rate_vs_tariff_group_avg_avg_6_prior_rank,
    rows_in_tariff_group_2,
    last_valid_from_2
FROM windowed
ORDER BY tariff_distinct_tariff_group_2_in_tariff_group_s NULLS LAST, id
LIMIT 10
