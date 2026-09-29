WITH recent_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        DAYOFWEEK(commissioned_on) AS commissioned_on_dayofweek,
        DATE_FORMAT(commissioned_on, 'yyyy-MM') AS commissioned_on_label,
        DATE_FORMAT(commissioned_on, 'yyyy-MM') AS commissioned_on_label_2
    FROM sites
    WHERE tariff NOT IN ('TOU', 'EV')
      AND installed_kw <= 386.6
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.id) AS reading_count,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.id) AS first_reading_read_at
    FROM recent_sites s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_commissioned_on_month_commis AS first_reading_read_at_rank,
        MIN(installed_kw) OVER (PARTITION BY MONTH(commissioned_on), commissioned_on_dayofweek ORDER BY first_reading_read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_installed_kw_so_far,
        ROUND(reading_count - AVG(reading_count) OVER (PARTITION BY tariff), 2) AS reading_count_vs_tariff_avg,
        LEAD(reading_count, 1, 0) OVER w_commissioned_on_dayofweek_zo AS next_reading_count,
        ROW_NUMBER() OVER (PARTITION BY MONTH(commissioned_on), commissioned_on_dayofweek ORDER BY first_reading_read_at ASC, id) AS first_reading_read_at_seq
    FROM summarized_rows
    WINDOW w_commissioned_on_month_commis AS (PARTITION BY MONTH(commissioned_on), commissioned_on_dayofweek ORDER BY first_reading_read_at ASC), w_commissioned_on_dayofweek_zo AS (PARTITION BY commissioned_on_dayofweek, zone ORDER BY commissioned_on ASC, id)
),
latest AS (
    -- Keep the earliest row per tariff, zone
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        commissioned_on_dayofweek,
        commissioned_on_label,
        commissioned_on_label_2,
        reading_count,
        first_reading_read_at,
        first_reading_read_at_rank,
        min_installed_kw_so_far,
        reading_count_vs_tariff_avg,
        next_reading_count,
        first_reading_read_at_seq
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff, zone ORDER BY reading_count_vs_tariff_avg, id) AS rn
        FROM sequenced
    ) ranked
    WHERE rn = 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY commissioned_on_dayofweek DESC), 3) AS commissioned_on_dayofweek_percent_rank,
        FIRST_VALUE(min_installed_kw_so_far) OVER (ORDER BY reading_count_vs_tariff_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_min_installed_kw_so_far,
        SUM(reading_count_vs_tariff_avg) OVER (PARTITION BY zone) AS zone_sum_reading_count_vs_tariff_avg,
        MAX(reading_count_vs_tariff_avg) OVER (PARTITION BY zone) AS zone_max_reading_count_vs_tariff_avg
    FROM latest
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at
    FROM with_trends s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        CONCAT(zone, '#', CAST(id AS STRING)) AS zone_ref
    FROM with_counts
    WHERE commissioned_on BETWEEN DATE '2024-08-24' AND DATE '2025-05-10'
)
SELECT *
FROM (
    SELECT commissioned_on_dayofweek, tariff, first_reading_read_at_seq
    FROM tagged
) src
PIVOT (
    SUM(first_reading_read_at_seq) AS total, MAX(first_reading_read_at_seq) AS max_v
    FOR tariff IN ('FLAT', 'TOU')
)
ORDER BY commissioned_on_dayofweek
