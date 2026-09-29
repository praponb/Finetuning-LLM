WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour
    FROM meter_readings
    WHERE site_id >= 6
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(read_at) OVER (PARTITION BY MONTH(read_at) ORDER BY read_at_hour ASC, id) AS prev_read_at,
        SUM(kwh) OVER (ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_sum_2_prior,
        RANK() OVER w_site_id_by_read_at_hour_ties AS read_at_hour_rank,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id,
        SUM(kwh) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        ROUND(CUME_DIST() OVER w_read_at_month_by_read_at_hou, 3) AS read_at_hour_cume_dist,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_id ORDER BY read_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        LAST_VALUE(read_at_hour) IGNORE NULLS OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at_hour,
        ROUND(AVG(kwh) OVER (ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_3
    FROM prepared
    WINDOW w_site_id_by_read_at_hour_ties AS (PARTITION BY site_id ORDER BY read_at_hour), w_site_id AS (PARTITION BY site_id), w_read_at_month_by_read_at_hou AS (PARTITION BY MONTH(read_at) ORDER BY read_at_hour ASC, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(prev_read_at, 3) OVER (PARTITION BY site_id, read_at_hour_rank ORDER BY running_kwh DESC, id) AS next3_prev_read_at,
        LAG(read_at_hour_rank) OVER (PARTITION BY site_id ORDER BY rows_in_site_id DESC, id) AS prev_read_at_hour_rank,
        ROUND(AVG(rows_in_site_id) OVER (PARTITION BY MONTH(read_at_hour)), 2) AS read_at_hour_month_avg_rows_in_site_id,
        MIN(kwh_sum_2_prior) OVER (PARTITION BY read_at_hour_rank) AS read_at_hour_rank_min_kwh_sum_2_prior
    FROM ranked
)
SELECT
    id,
    site_id,
    kwh,
    read_at_hour,
    prev_read_at,
    kwh_sum_2_prior,
    rows_in_site_id,
    voltage_share_of_site_id,
    running_kwh,
    voltage_vs_site_id_avg,
    second_kwh,
    last_known_read_at_hour,
    kwh_avg_3,
    next3_prev_read_at,
    prev_read_at_hour_rank,
    read_at_hour_month_avg_rows_in_site_id,
    read_at_hour_rank_min_kwh_sum_2_prior,
    ROUND((UNIX_TIMESTAMP(read_at_hour) - UNIX_TIMESTAMP(last_known_read_at_hour)) / 3600, 1) AS hours_last_known_read_at_hour_to_read_at_hour,
    ROUND(kwh_sum_2_prior / NULLIF(voltage_vs_site_id_avg, 0), 3) AS kwh_sum_2_prior_per_voltage_vs_site_id_avg
FROM windowed
