WITH valid_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        installed_kw,
        commissioned_on,
        CONCAT(zone, '#', CAST(id AS STRING)) AS zone_ref,
        CASE WHEN tariff IN ('TOU', 'FLAT') THEN 'group_a' WHEN tariff = 'EV' THEN 'group_b' ELSE 'other' END AS tariff_group
    FROM sites
    WHERE installed_kw BETWEEN 93.3 AND 284.0
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        site_name,
        installed_kw,
        zone_ref,
        MAX(installed_kw) OVER w_commissioned_on_month_tariff AS commissioned_on_month_tariff_group_max_installed,
        LEAD(installed_kw, 3) OVER (PARTITION BY tariff_group, DATE_TRUNC('MONTH', commissioned_on) ORDER BY installed_kw DESC, id) AS next3_installed_kw,
        ROUND(AVG(installed_kw) OVER w_commissioned_on_month_tariff, 2) AS commissioned_on_month_tariff_group_avg_installed,
        SUM(installed_kw) OVER (PARTITION BY tariff_group, DATE_TRUNC('MONTH', commissioned_on) ORDER BY installed_kw DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS installed_kw_sum_4,
        SUM(installed_kw) OVER (PARTITION BY MONTH(commissioned_on), tariff_group) AS commissioned_on_month_tariff_group_sum_installed
    FROM valid_sites
    WINDOW w_commissioned_on_month_tariff AS (PARTITION BY MONTH(commissioned_on), tariff_group)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(installed_kw_sum_4, 2) OVER (ORDER BY installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_installed_kw_sum_4,
        installed_kw_sum_4 - LAG(installed_kw_sum_4) OVER (ORDER BY commissioned_on_month_tariff_group_max_installed, id) AS installed_kw_sum_4_change
    FROM ranked
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        site_name,
        installed_kw,
        zone_ref,
        commissioned_on_month_tariff_group_max_installed,
        next3_installed_kw,
        commissioned_on_month_tariff_group_avg_installed,
        installed_kw_sum_4,
        commissioned_on_month_tariff_group_sum_installed,
        installed_kw_sum_4_change,
        RANK() OVER w_all_by_commissioned_on_month_2 AS commissioned_on_month_tariff_group_sum_installed_2,
        SUM(next3_installed_kw) OVER (ORDER BY commissioned_on_month_tariff_group_sum_installed ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next3_installed_kw_sum_3,
        NTILE(4) OVER (ORDER BY commissioned_on_month_tariff_group_sum_installed ASC, id) AS commissioned_on_month_tariff_group_sum_installed_3,
        FIRST_VALUE(installed_kw_sum_4) IGNORE NULLS OVER (ORDER BY commissioned_on_month_tariff_group_sum_installed ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_installed_kw_sum_4,
        LAG(installed_kw) OVER w_all_by_commissioned_on_month_2 AS prev_installed_kw,
        LAST_VALUE(installed_kw_sum_4) IGNORE NULLS OVER (ORDER BY commissioned_on_month_tariff_group_sum_installed ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_installed_kw_sum_4
    FROM sequenced
    WINDOW w_all_by_commissioned_on_month_2 AS (ORDER BY commissioned_on_month_tariff_group_sum_installed ASC, id)
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        installed_kw,
        commissioned_on_month_tariff_group_max_installed,
        commissioned_on_month_tariff_group_sum_installed,
        commissioned_on_month_tariff_group_sum_installed_2,
        next3_installed_kw_sum_3,
        prev_installed_kw,
        last_known_installed_kw_sum_4,
        ROUND(installed_kw - AVG(installed_kw) OVER (PARTITION BY commissioned_on_month_tariff_group_sum_installed_2, commissioned_on_month_tariff_group_sum_installed_3), 2) AS installed_kw_vs_commissioned_on_month_tariff_gro,
        MAX(installed_kw_sum_4) OVER w_commissioned_on_month_tariff_2 AS commissioned_on_month_tariff_group_sum_installed_4,
        LEAD(prev_installed_kw) OVER (PARTITION BY commissioned_on_month_tariff_group_sum_installed_3, commissioned_on_month_tariff_group_sum_installed_2 ORDER BY installed_kw_sum_4_change DESC, id) AS next_prev_installed_kw,
        ROUND(AVG(next3_installed_kw_sum_3) OVER w_commissioned_on_month_tariff_2, 2) AS commissioned_on_month_tariff_group_sum_installed_5,
        LAG(installed_kw_sum_4, 1, 0) OVER (PARTITION BY commissioned_on_month_tariff_group_sum_installed_3, commissioned_on_month_tariff_group_sum_installed_2 ORDER BY installed_kw_sum_4_change DESC, id) AS prev_installed_kw_sum_4,
        ROUND(CUME_DIST() OVER (PARTITION BY commissioned_on_month_tariff_group_sum_installed_3, commissioned_on_month_tariff_group_sum_installed_2 ORDER BY installed_kw_sum_4_change DESC, id), 3) AS installed_kw_sum_4_change_cume_dist
    FROM with_trends
    WINDOW w_commissioned_on_month_tariff_2 AS (PARTITION BY commissioned_on_month_tariff_group_sum_installed_2, commissioned_on_month_tariff_group_sum_installed_3)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        commissioned_on_month_tariff_group_sum_installed,
        next3_installed_kw_sum_3,
        prev_installed_kw,
        last_known_installed_kw_sum_4,
        installed_kw_vs_commissioned_on_month_tariff_gro,
        next_prev_installed_kw,
        commissioned_on_month_tariff_group_sum_installed_5,
        ROW_NUMBER() OVER (PARTITION BY commissioned_on_month_tariff_group_sum_installed_2 ORDER BY next3_installed_kw_sum_3, id) AS next3_installed_kw_sum_3_seq,
        ROUND(AVG(installed_kw) OVER (PARTITION BY commissioned_on_month_tariff_group_sum_installed_2), 2) AS commissioned_on_month_tariff_group_sum_installed_3,
        MAX(next3_installed_kw_sum_3) OVER (PARTITION BY commissioned_on_month_tariff_group_sum_installed_2) AS commissioned_on_month_tariff_group_sum_installed_6,
        DENSE_RANK() OVER (PARTITION BY commissioned_on_month_tariff_group_sum_installed_2 ORDER BY next3_installed_kw_sum_3) AS next3_installed_kw_sum_3_dense_rank
    FROM sequenced_2
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        commissioned_on_month_tariff_group_sum_installed,
        next3_installed_kw_sum_3,
        prev_installed_kw,
        last_known_installed_kw_sum_4,
        next_prev_installed_kw,
        next3_installed_kw_sum_3_seq,
        commissioned_on_month_tariff_group_sum_installed_3,
        commissioned_on_month_tariff_group_sum_installed_6,
        next3_installed_kw_sum_3 + prev_installed_kw AS next3_installed_kw_sum_3_plus_prev_installed_kw,
        LEAST(next_prev_installed_kw, prev_installed_kw) AS least_next_prev_installed_kw_prev_installed_kw,
        LEAST(next_prev_installed_kw, prev_installed_kw) AS least_next_prev_installed_kw_prev_installed_kw_2
    FROM metrics
    WHERE next3_installed_kw_sum_3_seq BETWEEN 4 AND 9
)
SELECT
    id,
    commissioned_on_month_tariff_group_sum_installed,
    next3_installed_kw_sum_3,
    last_known_installed_kw_sum_4,
    next3_installed_kw_sum_3_seq,
    commissioned_on_month_tariff_group_sum_installed_3,
    commissioned_on_month_tariff_group_sum_installed_6,
    next3_installed_kw_sum_3_plus_prev_installed_kw,
    least_next_prev_installed_kw_prev_installed_kw_2,
    FLOOR(next_prev_installed_kw / 100) * 100 AS next_prev_installed_kw_bucket,
    ROUND(prev_installed_kw / NULLIF(next3_installed_kw_sum_3, 0), 3) AS prev_installed_kw_per_next3_installed_kw_sum_3
FROM annotated
ORDER BY next3_installed_kw_sum_3_plus_prev_installed_kw DESC, commissioned_on_month_tariff_group_sum_installed_3
