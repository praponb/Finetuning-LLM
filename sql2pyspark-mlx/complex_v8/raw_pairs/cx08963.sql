WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE voltage >= 221.4
      AND quality IN ('BAD', 'OK')
),
with_parent AS (
    -- Attach site attributes
    SELECT s.*, si.commissioned_on AS site_commissioned_on
    FROM cleaned s
    LEFT JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(kwh - AVG(kwh) OVER w_read_at_month_site_id, 2) AS kwh_vs_read_at_month_site_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, quality ORDER BY read_at NULLS LAST), 3) AS read_at_percent_rank,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_quality, 0), 4) AS kwh_share_of_quality,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_id, quality ORDER BY read_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        RANK() OVER (PARTITION BY site_id, quality ORDER BY read_at NULLS LAST) AS read_at_rank,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY quality, site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        SUM(kwh) OVER (PARTITION BY quality) AS quality_sum_kwh,
        LAST_VALUE(site_commissioned_on) OVER (PARTITION BY quality ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_commissioned_on
    FROM with_parent
    WINDOW w_read_at_month_site_id AS (PARTITION BY MONTH(read_at), site_id), w_quality AS (PARTITION BY quality)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        site_commissioned_on,
        kwh_vs_read_at_month_site_id_avg,
        read_at_percent_rank,
        kwh_share_of_quality,
        second_read_at,
        second_kwh,
        quality_sum_kwh,
        last_site_commissioned_on
    FROM ranked
    WHERE read_at_rank <= 2
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    site_commissioned_on,
    kwh_vs_read_at_month_site_id_avg,
    read_at_percent_rank,
    kwh_share_of_quality,
    second_kwh,
    quality_sum_kwh,
    last_site_commissioned_on
FROM top_rows
WHERE site_commissioned_on >= DATE '2024-06-27'
ORDER BY site_id DESC
