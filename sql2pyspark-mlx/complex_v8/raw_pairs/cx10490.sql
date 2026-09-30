WITH filtered_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket,
        DATE_FORMAT(commissioned_on, 'MMM yyyy') AS commissioned_on_label
    FROM sites
    WHERE UPPER(TRIM(site_name)) LIKE 'S%'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(installed_kw, 3) OVER (ORDER BY commissioned_on, id) AS next3_installed_kw,
        RANK() OVER (PARTITION BY MONTH(commissioned_on), zone ORDER BY installed_kw) AS installed_kw_rank,
        SUM(installed_kw) OVER (PARTITION BY tariff) AS tariff_sum_installed_kw,
        ROUND(PERCENT_RANK() OVER (ORDER BY commissioned_on, id), 3) AS commissioned_on_percent_rank,
        NTH_VALUE(commissioned_on, 2) OVER (ORDER BY installed_kw_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_commissioned_on,
        COUNT(*) OVER (PARTITION BY tariff, zone) AS rows_in_tariff_zone,
        ROUND(PERCENT_RANK() OVER (ORDER BY installed_kw NULLS LAST, id), 3) AS installed_kw_percent_rank,
        COUNT(*) OVER (ORDER BY commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM filtered_sites
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(installed_kw_bucket) OVER (ORDER BY commissioned_on DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS installed_kw_bucket_sum_1_prior,
        NTH_VALUE(running_count, 2) OVER (ORDER BY commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_count,
        commissioned_on_percent_rank - LAG(commissioned_on_percent_rank) OVER (PARTITION BY installed_kw_rank, DATE_TRUNC('MONTH', commissioned_on) ORDER BY commissioned_on DESC, id) AS commissioned_on_percent_rank_change,
        ROUND(CUME_DIST() OVER (ORDER BY commissioned_on DESC, id), 3) AS commissioned_on_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY installed_kw_rank, DATE_TRUNC('MONTH', commissioned_on) ORDER BY commissioned_on DESC, id) AS rn,
        COUNT(*) OVER (PARTITION BY tariff) AS rows_in_tariff,
        ROUND(tariff_sum_installed_kw / NULLIF(SUM(tariff_sum_installed_kw) OVER (PARTITION BY tariff), 0), 4) AS tariff_sum_installed_kw_share_of_tariff
    FROM metrics
    WINDOW w_installed_kw_rank_commission AS (PARTITION BY installed_kw_rank, DATE_TRUNC('MONTH', commissioned_on) ORDER BY commissioned_on DESC, id)
)
SELECT
    id,
    site_name,
    zone,
    tariff,
    commissioned_on,
    commissioned_on_label,
    installed_kw_rank,
    tariff_sum_installed_kw,
    commissioned_on_percent_rank,
    second_commissioned_on,
    rows_in_tariff_zone,
    installed_kw_percent_rank,
    running_count,
    installed_kw_bucket_sum_1_prior,
    second_running_count,
    commissioned_on_percent_rank_change,
    commissioned_on_cume_dist,
    rn,
    rows_in_tariff,
    tariff_sum_installed_kw_share_of_tariff,
    LEAST(installed_kw_rank, tariff_sum_installed_kw) AS least_installed_kw_rank_tariff_sum_installed_kw,
    LENGTH(TRIM(tariff)) AS tariff_len
FROM ranked
ORDER BY commissioned_on_label NULLS LAST, id
LIMIT 10
