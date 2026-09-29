WITH recent_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        installed_kw,
        commissioned_on,
        COALESCE(installed_kw, 0.0) AS installed_kw_filled
    FROM sites
    WHERE UPPER(TRIM(site_name)) LIKE 'S%'
      AND installed_kw < 373.3
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(installed_kw, 3) OVER (ORDER BY commissioned_on DESC, id) AS prev3_installed_kw,
        installed_kw - LAG(installed_kw) OVER (ORDER BY commissioned_on DESC, id) AS installed_kw_change,
        ROUND(AVG(installed_kw_filled) OVER (ORDER BY commissioned_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS installed_kw_filled_avg_4,
        RANK() OVER (ORDER BY commissioned_on, id) AS commissioned_on_rank,
        ROW_NUMBER() OVER (ORDER BY installed_kw_filled, id) AS seq_no,
        LEAD(installed_kw_filled) OVER (ORDER BY installed_kw_filled, id) AS next_installed_kw_filled,
        ROUND(AVG(installed_kw_filled) OVER (ORDER BY commissioned_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS installed_kw_filled_avg_3,
        ROUND(PERCENT_RANK() OVER (ORDER BY commissioned_on DESC), 3) AS commissioned_on_percent_rank,
        MAX(installed_kw) OVER (ORDER BY installed_kw_filled, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS installed_kw_max_2_prior,
        SUM(installed_kw_filled) OVER (ORDER BY commissioned_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS installed_kw_filled_sum_3
    FROM recent_sites
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_name,
        installed_kw,
        installed_kw_filled,
        installed_kw_change,
        installed_kw_filled_avg_4,
        commissioned_on_rank,
        seq_no,
        installed_kw_max_2_prior,
        ROUND(prev3_installed_kw - AVG(prev3_installed_kw) OVER w_seq_no_commissioned_on_rank, 2) AS prev3_installed_kw_vs_seq_no_commissioned_on_ran,
        COUNT(*) OVER w_seq_no_commissioned_on_rank AS rows_in_seq_no_commissioned_on_rank,
        SUM(commissioned_on_percent_rank) OVER (PARTITION BY seq_no, DATE_TRUNC('MONTH', commissioned_on) ORDER BY commissioned_on_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_commissioned_on_percent_rank,
        MIN(installed_kw_filled) OVER (PARTITION BY seq_no, DATE_TRUNC('MONTH', commissioned_on) ORDER BY commissioned_on_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_installed_kw_filled_so_far,
        MIN(commissioned_on_rank) OVER w_seq_no_commissioned_on_rank AS seq_no_commissioned_on_rank_min_commissioned_on_
    FROM with_trends
    WINDOW w_seq_no_commissioned_on_rank AS (PARTITION BY seq_no, commissioned_on_rank)
)
SELECT
    id,
    installed_kw,
    installed_kw_filled,
    installed_kw_change,
    installed_kw_filled_avg_4,
    commissioned_on_rank,
    seq_no,
    installed_kw_max_2_prior,
    prev3_installed_kw_vs_seq_no_commissioned_on_ran,
    rows_in_seq_no_commissioned_on_rank,
    running_commissioned_on_percent_rank,
    min_installed_kw_filled_so_far,
    SUBSTRING(TRIM(site_name), 1, 3) AS site_name_prefix,
    FLOOR(installed_kw_filled / 100) * 100 AS installed_kw_filled_bucket
FROM metrics
ORDER BY prev3_installed_kw_vs_seq_no_commissioned_on_ran
