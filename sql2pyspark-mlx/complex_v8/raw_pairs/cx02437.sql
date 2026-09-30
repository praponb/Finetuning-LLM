WITH joined_ranges AS (
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
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        quality,
        zone,
        rate,
        energy_cost,
        MIN(kwh) OVER (PARTITION BY site_id) AS site_id_min_kwh,
        LEAD(read_at, 3) OVER w_read_at_year_site_id_by_rate AS next3_read_at,
        NTH_VALUE(rate, 2) OVER (PARTITION BY YEAR(read_at), site_id ORDER BY rate, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rate,
        ROUND(CUME_DIST() OVER w_read_at_year_site_id_by_rate_3, 3) AS rate_cume_dist,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        DENSE_RANK() OVER w_read_at_year_site_id_by_rate_3 AS rate_dense_rank
    FROM joined_ranges
    WINDOW w_read_at_year_site_id_by_rate AS (PARTITION BY YEAR(read_at), site_id ORDER BY rate, id), w_read_at_year_site_id_by_rate_3 AS (PARTITION BY YEAR(read_at), site_id ORDER BY rate)
)
-- Final projection
SELECT
    id,
    kwh,
    quality,
    rate,
    energy_cost,
    site_id_min_kwh,
    next3_read_at,
    second_rate,
    rate_cume_dist,
    rows_in_site_id,
    rate_dense_rank,
    ROUND(rate / NULLIF(site_id_min_kwh, 0), 3) AS rate_per_site_id_min_kwh
FROM ranked
WHERE rows_in_site_id >= 6
ORDER BY id DESC, second_rate DESC
