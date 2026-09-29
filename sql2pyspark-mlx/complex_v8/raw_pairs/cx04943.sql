WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND kwh >= 52.4
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.read_at_label,
        si.commissioned_on AS site_commissioned_on
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id AND si.zone NOT IN ('Z2', 'Z3')
    WHERE si.commissioned_on BETWEEN DATE '2024-09-15' AND DATE '2025-05-10'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        quality,
        site_commissioned_on,
        NTH_VALUE(site_commissioned_on, 2) OVER (PARTITION BY quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_commissioned_on,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_site_id, 0), 4) AS kwh_share_of_site_id,
        NTH_VALUE(site_commissioned_on, 2) OVER (PARTITION BY quality, site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_commissioned_on_2,
        MAX(kwh) OVER (ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        ROUND(kwh - AVG(kwh) OVER w_site_id, 2) AS kwh_vs_site_id_avg,
        NTH_VALUE(kwh, 2) OVER (ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        LAG(kwh, 1, 0) OVER (PARTITION BY quality ORDER BY kwh DESC, id) AS prev_kwh,
        LAST_VALUE(site_commissioned_on) OVER (ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_commissioned_on,
        FIRST_VALUE(site_commissioned_on) IGNORE NULLS OVER (ORDER BY site_commissioned_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_commissioned_on
    FROM with_details_site
    WINDOW w_site_id AS (PARTITION BY site_id)
)
SELECT
    id,
    site_id,
    kwh,
    site_commissioned_on,
    second_site_commissioned_on,
    kwh_share_of_site_id,
    max_kwh_so_far,
    kwh_vs_site_id_avg,
    second_kwh,
    prev_kwh,
    last_site_commissioned_on,
    first_known_site_commissioned_on,
    CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
FROM ranked
