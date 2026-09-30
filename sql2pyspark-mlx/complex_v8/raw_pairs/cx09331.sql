WITH filtered AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_to,
        rate,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        ROUND(rate * 0.25, 2) AS rate_adj,
        CASE WHEN tariff IN ('TOU', 'FLAT') THEN 'group_a' WHEN tariff = 'EV' THEN 'group_b' ELSE 'other' END AS tariff_group
    FROM tariff_windows
    WHERE rate < 6.1
      AND valid_to < TIMESTAMP '2025-03-12 23:00:00'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY tariff_or_unknown, tariff_group ORDER BY rate DESC) AS rate_dense_rank,
        SIZE(COLLECT_SET(tariff) OVER (PARTITION BY tariff_group)) AS distinct_tariff_in_tariff_group
    FROM filtered
),
latest AS (
    -- Keep the earliest row per tariff_or_unknown
    SELECT
        id,
        tariff,
        valid_to,
        rate,
        tariff_or_unknown,
        rate_adj,
        tariff_group,
        rate_dense_rank,
        distinct_tariff_in_tariff_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff_or_unknown ORDER BY rate_adj, id) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        valid_to,
        rate,
        tariff_or_unknown,
        rate_adj,
        tariff_group,
        NTILE(3) OVER (PARTITION BY distinct_tariff_in_tariff_group, TO_DATE(valid_to) ORDER BY distinct_tariff_in_tariff_group DESC NULLS FIRST, id) AS distinct_tariff_in_tariff_group_tercile,
        ROUND(rate - AVG(rate) OVER (PARTITION BY rate_dense_rank), 2) AS rate_vs_rate_dense_rank_avg,
        LEAD(rate_dense_rank, 1, 0) OVER (PARTITION BY distinct_tariff_in_tariff_group, TO_DATE(valid_to) ORDER BY distinct_tariff_in_tariff_group DESC NULLS FIRST, id) AS next_rate_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY distinct_tariff_in_tariff_group, TO_DATE(valid_to) ORDER BY distinct_tariff_in_tariff_group DESC NULLS FIRST, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_tariff_in_tariff_group, TO_DATE(valid_to) ORDER BY distinct_tariff_in_tariff_group DESC NULLS FIRST, id), 3) AS distinct_tariff_in_tariff_group_percent_rank
    FROM latest
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE seq_no = 1
),
filtered_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM top_per_group
    WHERE rate_vs_rate_dense_rank_avg > (SELECT MIN(rate_vs_rate_dense_rank_avg) FROM top_per_group)
)
SELECT
    id,
    valid_to,
    rate,
    tariff_or_unknown,
    rate_adj,
    tariff_group,
    rate_vs_rate_dense_rank_avg,
    seq_no,
    distinct_tariff_in_tariff_group_percent_rank
FROM filtered_2
