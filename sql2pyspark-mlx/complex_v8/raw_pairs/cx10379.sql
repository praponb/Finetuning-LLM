WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        LENGTH(TRIM(site_name)) AS site_name_len,
        DATE_ADD(commissioned_on, 90) AS commissioned_on_plus_90d
    FROM sites
    WHERE installed_kw BETWEEN 93.3 AND 373.3
      AND commissioned_on >= DATE '2024-06-27'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(installed_kw) OVER (PARTITION BY zone) AS zone_max_installed_kw,
        FIRST_VALUE(installed_kw) IGNORE NULLS OVER (PARTITION BY site_name_len ORDER BY installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_installed_kw,
        ROW_NUMBER() OVER w_site_name_len_by_installed_k_2 AS seq_no
    FROM recent
    WINDOW w_site_name_len_by_installed_k_2 AS (PARTITION BY site_name_len ORDER BY installed_kw, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        site_name,
        zone,
        commissioned_on,
        site_name_len,
        commissioned_on_plus_90d,
        seq_no,
        FLOOR(first_known_installed_kw / 100) * 100 AS first_known_installed_kw_bucket,
        DATE_TRUNC('MONTH', commissioned_on_plus_90d) AS commissioned_on_plus_90d_month,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown
    FROM windowed
)
SELECT id, metric, reading
FROM (
    SELECT id, site_name_len, first_known_installed_kw_bucket, seq_no
    FROM labeled
) src
UNPIVOT (
    reading FOR metric IN (site_name_len, first_known_installed_kw_bucket, seq_no)
) unp
