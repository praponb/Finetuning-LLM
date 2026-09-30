WITH in_window AS (
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
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(energy_cost / NULLIF(SUM(energy_cost) OVER w_read_at_month_site_id, 0), 4) AS energy_cost_share_of_read_at_month_site_id,
        DENSE_RANK() OVER (ORDER BY rate) AS rate_dense_rank,
        LEAD(kwh) OVER (ORDER BY rate, id) AS next_kwh,
        FIRST_VALUE(rate) OVER (ORDER BY rate, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rate,
        ROUND(CUME_DIST() OVER (ORDER BY rate), 3) AS rate_cume_dist
    FROM in_window
    WINDOW w_read_at_month_site_id AS (PARTITION BY MONTH(read_at), site_id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(energy_cost) OVER (PARTITION BY site_id, quality) AS site_id_quality_sum_energy_cost,
        ROUND(CUME_DIST() OVER (PARTITION BY rate_dense_rank, site_id ORDER BY first_rate ASC), 3) AS first_rate_cume_dist,
        MIN(rate_dense_rank) OVER (PARTITION BY site_id, quality) AS site_id_quality_min_rate_dense_rank,
        SIZE(COLLECT_SET(zone) OVER w_site_id_quality) AS distinct_zone_in_site_id_quality
    FROM ranked
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    zone,
    energy_cost,
    energy_cost_share_of_read_at_month_site_id,
    next_kwh,
    first_rate,
    site_id_quality_sum_energy_cost,
    first_rate_cume_dist,
    distinct_zone_in_site_id_quality,
    CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
FROM with_trends
WHERE first_rate_cume_dist <= 0.8
ORDER BY quality_group DESC
