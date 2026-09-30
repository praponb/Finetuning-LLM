WITH matched AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        COUNT(*) OVER (PARTITION BY quality ORDER BY energy_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM matched
),
latest_per_key AS (
    -- Keep the latest row per zone
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        zone,
        rate,
        energy_cost,
        rows_in_site_id,
        running_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone ORDER BY read_at DESC, id DESC) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(rate) OVER (PARTITION BY site_id), 2) AS site_id_avg_rate,
        DENSE_RANK() OVER (ORDER BY running_count DESC) AS running_count_dense_rank
    FROM latest_per_key
)
-- Subtotals with GROUPING SETS
SELECT
    running_count_dense_rank,
    COALESCE(zone, 'ALL') AS zone,
    COALESCE(quality, 'ALL') AS quality,
    GROUPING_ID() AS gid,
    MIN(read_at) AS first_read_at,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
FROM metrics
GROUP BY GROUPING SETS ((running_count_dense_rank, zone, quality), (running_count_dense_rank), (zone), ())
ORDER BY running_count_dense_rank NULLS LAST, zone NULLS LAST, quality NULLS LAST
