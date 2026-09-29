WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        ROUND(installed_kw * 0.25, 2) AS installed_kw_adj
    FROM sites
    WHERE commissioned_on < DATE '2024-11-21'
      AND tariff = 'FLAT'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        installed_kw_adj,
        DENSE_RANK() OVER (PARTITION BY tariff, zone ORDER BY installed_kw_adj DESC) AS installed_kw_adj_dense_rank,
        FIRST_VALUE(installed_kw_adj) OVER (PARTITION BY tariff ORDER BY installed_kw_adj ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_installed_kw_adj,
        FIRST_VALUE(installed_kw) IGNORE NULLS OVER (ORDER BY installed_kw ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_installed_kw,
        installed_kw_adj - LAG(installed_kw_adj) OVER (ORDER BY installed_kw ASC NULLS LAST, id) AS installed_kw_adj_change,
        FIRST_VALUE(installed_kw) OVER (PARTITION BY tariff ORDER BY installed_kw_adj ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_installed_kw,
        SUM(installed_kw) OVER (PARTITION BY tariff, zone ORDER BY installed_kw_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_installed_kw,
        LAST_VALUE(installed_kw) OVER (PARTITION BY tariff, zone ORDER BY installed_kw_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_installed_kw,
        SUM(installed_kw) OVER (ORDER BY installed_kw ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_installed_kw_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY installed_kw ASC NULLS LAST, id), 3) AS installed_kw_percent_rank,
        ROUND(installed_kw_adj / NULLIF(SUM(installed_kw_adj) OVER (PARTITION BY tariff, zone), 0), 4) AS installed_kw_adj_share_of_tariff_zone,
        ROUND(AVG(installed_kw_adj) OVER (PARTITION BY tariff ORDER BY installed_kw_adj ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS installed_kw_adj_avg_2,
        ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY installed_kw_adj ASC, id) AS seq_no
    FROM cleaned
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        installed_kw,
        installed_kw_adj,
        first_installed_kw_adj,
        installed_kw_adj_change,
        first_installed_kw,
        last_installed_kw,
        installed_kw_adj_avg_2,
        seq_no,
        ABS(first_installed_kw_adj - 414.8) AS first_installed_kw_adj_distance,
        SUBSTRING(TRIM(site_name), 1, 3) AS site_name_prefix
    FROM ranked
    WHERE installed_kw_adj < 526.5
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        installed_kw_adj,
        first_installed_kw_adj,
        installed_kw_adj_change,
        last_installed_kw,
        installed_kw_adj_avg_2,
        seq_no,
        first_installed_kw_adj_distance,
        ROUND(CUME_DIST() OVER w_seq_no_by_first_installed_kw, 3) AS first_installed_kw_adj_cume_dist,
        LAG(first_installed_kw, 3) OVER w_seq_no_by_first_installed_kw AS prev3_first_installed_kw,
        ROUND(last_installed_kw - AVG(last_installed_kw) OVER (PARTITION BY seq_no), 2) AS last_installed_kw_vs_seq_no_avg,
        LAG(seq_no) OVER (PARTITION BY seq_no ORDER BY first_installed_kw_adj_distance, id) AS prev_seq_no,
        FIRST_VALUE(seq_no) IGNORE NULLS OVER (PARTITION BY seq_no ORDER BY first_installed_kw_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        FIRST_VALUE(installed_kw) OVER (PARTITION BY seq_no ORDER BY first_installed_kw_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_installed_kw_2,
        LAG(installed_kw_adj, 2) OVER (PARTITION BY seq_no ORDER BY first_installed_kw_adj, id) AS prev2_installed_kw_adj
    FROM scored
    WINDOW w_seq_no_by_first_installed_kw AS (PARTITION BY seq_no ORDER BY first_installed_kw_adj, id)
)
-- Final projection
SELECT
    id,
    first_installed_kw_adj,
    last_installed_kw,
    installed_kw_adj_avg_2,
    seq_no,
    first_installed_kw_adj_cume_dist,
    last_installed_kw_vs_seq_no_avg,
    first_installed_kw_2,
    prev2_installed_kw_adj,
    COALESCE(installed_kw_adj_change, 0.0) AS installed_kw_adj_change_filled
FROM windowed
WHERE prev_seq_no < 7
