WITH covered AS (
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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        kwh,
        quality,
        zone,
        rate,
        energy_cost,
        ROW_NUMBER() OVER w_zone_read_at_month_by_energy AS seq_no,
        ROUND(energy_cost / NULLIF(SUM(energy_cost) OVER (PARTITION BY quality, site_id), 0), 4) AS energy_cost_share_of_quality_site_id,
        RANK() OVER (PARTITION BY zone, MONTH(read_at) ORDER BY energy_cost) AS energy_cost_rank,
        SUM(energy_cost) OVER (PARTITION BY zone, MONTH(read_at) ORDER BY energy_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_energy_cost
    FROM covered
    WINDOW w_zone_read_at_month_by_energy AS (PARTITION BY zone, MONTH(read_at) ORDER BY energy_cost, id)
),
latest AS (
    -- Keep the latest row per quality
    SELECT
        id,
        kwh,
        quality,
        zone,
        rate,
        energy_cost,
        seq_no,
        energy_cost_share_of_quality_site_id,
        energy_cost_rank,
        running_energy_cost
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY energy_cost DESC, id DESC) AS recency_rank
        FROM ranked
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    energy_cost_rank,
    quality,
    GROUPING(energy_cost_rank) AS is_total_energy_cost_rank,
    ROUND(AVG(energy_cost_rank), 2) AS avg_energy_cost_rank
FROM latest
GROUP BY ROLLUP(energy_cost_rank, quality)
ORDER BY energy_cost_rank NULLS LAST, quality NULLS LAST
