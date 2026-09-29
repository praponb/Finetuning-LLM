WITH recent AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_name,
        tariff,
        commissioned_on,
        COUNT(*) OVER (PARTITION BY zone) AS rows_in_zone,
        RANK() OVER (PARTITION BY YEAR(commissioned_on) ORDER BY installed_kw DESC) AS installed_kw_rank,
        ROUND(installed_kw - AVG(installed_kw) OVER (PARTITION BY tariff), 2) AS installed_kw_vs_tariff_avg,
        ROUND(installed_kw / NULLIF(SUM(installed_kw) OVER w_tariff, 0), 4) AS installed_kw_share_of_tariff,
        SUM(installed_kw) OVER (PARTITION BY tariff) AS tariff_sum_installed_kw,
        RANK() OVER (PARTITION BY zone ORDER BY installed_kw, id) AS installed_kw_rank_2,
        ROW_NUMBER() OVER (ORDER BY installed_kw, id) AS seq_no,
        ROUND(AVG(installed_kw) OVER (PARTITION BY zone), 2) AS zone_avg_installed_kw,
        SUM(installed_kw) OVER (PARTITION BY zone) AS zone_sum_installed_kw,
        FIRST_VALUE(commissioned_on) OVER (ORDER BY commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_commissioned_on,
        COUNT(*) OVER (PARTITION BY tariff) AS rows_in_tariff,
        SUM(installed_kw) OVER (PARTITION BY zone ORDER BY installed_kw, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS installed_kw_sum_3,
        FIRST_VALUE(installed_kw) IGNORE NULLS OVER (PARTITION BY zone, tariff ORDER BY commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_installed_kw,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on) ORDER BY installed_kw, id) AS rn,
        ROUND(CUME_DIST() OVER (ORDER BY installed_kw), 3) AS installed_kw_cume_dist,
        DENSE_RANK() OVER (ORDER BY commissioned_on) AS commissioned_on_dense_rank
    FROM recent
    WINDOW w_tariff AS (PARTITION BY tariff)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(tariff_sum_installed_kw) OVER w_rn_tariff, 2) AS rn_tariff_avg_tariff_sum_installed_kw,
        MAX(tariff_sum_installed_kw) OVER w_rn_tariff AS rn_tariff_max_tariff_sum_installed_kw,
        ROUND(commissioned_on_dense_rank / NULLIF(SUM(commissioned_on_dense_rank) OVER w_rn_tariff, 0), 4) AS commissioned_on_dense_rank_share_of_rn_tariff,
        ROUND(AVG(rows_in_tariff) OVER (PARTITION BY commissioned_on_dense_rank ORDER BY installed_kw_share_of_tariff DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS rows_in_tariff_avg_2,
        COUNT(*) OVER (PARTITION BY rn, tariff) AS rows_in_rn_tariff,
        NTILE(10) OVER (PARTITION BY YEAR(first_commissioned_on) ORDER BY tariff_sum_installed_kw DESC, id) AS tariff_sum_installed_kw_decile,
        ROUND(rn / NULLIF(SUM(rn) OVER (PARTITION BY rn, tariff), 0), 4) AS rn_share_of_rn_tariff
    FROM with_windows
    WINDOW w_rn_tariff AS (PARTITION BY rn, tariff)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        installed_kw_rank_2,
        seq_no,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list,
        SUM(rows_in_zone) AS total_rows_in_zone,
        SUM(rows_in_tariff) AS total_rows_in_tariff,
        ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
        COUNT(*) AS row_count
    FROM metrics
    GROUP BY installed_kw_rank_2, seq_no
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        installed_kw_rank_2,
        seq_no,
        site_name_list,
        row_count,
        MAX(total_rows_in_zone) OVER (ORDER BY total_rows_in_tariff DESC NULLS FIRST, installed_kw_rank_2, seq_no ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS total_rows_in_zone_max_3,
        ROW_NUMBER() OVER w_all_by_total_rows_in_tariff_2 AS seq_no_2,
        ROUND(seq_no - AVG(seq_no) OVER (PARTITION BY row_count, seq_no), 2) AS seq_no_vs_row_count_seq_no_avg,
        NTH_VALUE(row_count, 2) OVER (PARTITION BY installed_kw_rank_2, seq_no ORDER BY total_rows_in_tariff, installed_kw_rank_2, seq_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_row_count
    FROM rollup_base
    WINDOW w_all_by_total_rows_in_tariff_2 AS (ORDER BY total_rows_in_tariff DESC NULLS FIRST, installed_kw_rank_2, seq_no)
)
-- Final projection
SELECT
    installed_kw_rank_2,
    seq_no,
    site_name_list,
    row_count,
    total_rows_in_zone_max_3,
    seq_no_2,
    seq_no_vs_row_count_seq_no_avg,
    second_row_count
FROM windowed
ORDER BY installed_kw_rank_2 DESC, second_row_count
