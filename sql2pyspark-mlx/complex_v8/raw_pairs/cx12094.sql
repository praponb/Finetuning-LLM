WITH valid AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket,
        CASE WHEN zone IN ('Z3', 'Z4') THEN 'group_a' WHEN zone = 'Z2' THEN 'group_b' ELSE 'other' END AS zone_group
    FROM sites
    WHERE commissioned_on BETWEEN DATE '2024-04-23' AND DATE '2025-11-28'
      AND UPPER(TRIM(site_name)) LIKE 'H%'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw_bucket,
        zone_group,
        MAX(installed_kw_bucket) OVER (PARTITION BY tariff ORDER BY commissioned_on, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS installed_kw_bucket_max_1_prior,
        COUNT(*) OVER (PARTITION BY zone) AS rows_in_zone,
        ROUND(installed_kw_bucket / NULLIF(SUM(installed_kw_bucket) OVER (PARTITION BY zone), 0), 4) AS installed_kw_bucket_share_of_zone,
        DATEDIFF(commissioned_on, LAG(commissioned_on) OVER (PARTITION BY zone, YEAR(commissioned_on) ORDER BY commissioned_on, id)) AS days_since_prev_commissioned_on,
        LEAD(installed_kw_bucket) OVER (ORDER BY installed_kw_bucket DESC, id) AS next_installed_kw_bucket,
        LAG(commissioned_on) OVER (PARTITION BY YEAR(commissioned_on) ORDER BY installed_kw_bucket ASC, id) AS prev_commissioned_on,
        LAST_VALUE(installed_kw_bucket) OVER (PARTITION BY zone, YEAR(commissioned_on) ORDER BY commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_installed_kw_bucket,
        ROUND(PERCENT_RANK() OVER (ORDER BY installed_kw_bucket DESC, id), 3) AS installed_kw_bucket_percent_rank,
        LAG(commissioned_on, 2) OVER (PARTITION BY zone_group, DATE_TRUNC('MONTH', commissioned_on) ORDER BY commissioned_on DESC, id) AS prev2_commissioned_on,
        LAG(installed_kw_bucket) OVER (PARTITION BY zone, YEAR(commissioned_on) ORDER BY commissioned_on, id) AS prev_installed_kw_bucket
    FROM valid
)
-- Final projection
SELECT
    id,
    zone,
    tariff,
    installed_kw_bucket,
    installed_kw_bucket_max_1_prior,
    rows_in_zone,
    days_since_prev_commissioned_on,
    prev_commissioned_on,
    prev2_commissioned_on,
    prev_installed_kw_bucket
FROM metrics
WHERE zone_group <> 'group_b'
ORDER BY id
