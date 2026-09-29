WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM meter_readings
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_dayofweek,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
    WHERE LENGTH(TRIM(si.site_name)) >= 7
),
latest_per_key AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        read_at_dayofweek,
        site_commissioned_on,
        site_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY site_commissioned_on DESC, id DESC) AS recency_rank
        FROM enriched_site
    ) ranked
    WHERE recency_rank = 1
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_dayofweek,
        s.site_commissioned_on,
        s.site_name,
        si.zone AS site_zone
    FROM latest_per_key s
    JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY read_at_dayofweek, quality) AS rows_in_read_at_dayofweek_quality,
        MAX(read_at_dayofweek) OVER (ORDER BY read_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS read_at_dayofweek_max_6_prior,
        SUM(kwh) OVER (ORDER BY read_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_sum_4,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_id, read_at_dayofweek ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at
    FROM joined_site
)
SELECT DISTINCT read_at_dayofweek, rows_in_read_at_dayofweek_quality, first_known_read_at
FROM with_windows
ORDER BY rows_in_read_at_dayofweek_quality, first_known_read_at DESC
