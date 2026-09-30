WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE quality IN ('EST', 'OK')
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.voltage_filled,
        si.tariff AS site_tariff
    FROM recent s
    LEFT JOIN sites si ON si.id = s.site_id AND si.zone NOT IN ('Z2', 'Z4')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        voltage_filled,
        RANK() OVER (PARTITION BY site_id ORDER BY voltage DESC, id) AS voltage_rank,
        COUNT(*) OVER (PARTITION BY TO_DATE(read_at)) AS rows_in_read_at_to_date,
        COUNT(*) OVER w_site_tariff AS rows_in_site_tariff,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_id ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        MIN(kwh) OVER (ORDER BY voltage_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY voltage DESC), 3) AS voltage_percent_rank,
        MIN(kwh) OVER (PARTITION BY site_tariff) AS site_tariff_min_kwh,
        RANK() OVER (ORDER BY voltage_filled, id) AS voltage_filled_rank,
        RANK() OVER (PARTITION BY site_tariff, site_id ORDER BY read_at) AS read_at_rank,
        RANK() OVER w_site_tariff_site_id_by_read_2 AS read_at_rank_2,
        ROW_NUMBER() OVER w_all_by_voltage_filled AS voltage_filled_seq,
        MAX(voltage) OVER (PARTITION BY site_tariff, site_id ORDER BY read_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_max_4,
        NTILE(5) OVER w_site_tariff_site_id_by_read_2 AS read_at_quintile,
        ROW_NUMBER() OVER (PARTITION BY site_tariff, site_id ORDER BY read_at, id) AS read_at_seq,
        LAG(voltage_filled) OVER (PARTITION BY site_tariff, site_id ORDER BY read_at, id) AS prev_voltage_filled
    FROM with_details_site
    WINDOW w_site_tariff AS (PARTITION BY site_tariff), w_site_tariff_site_id_by_read_2 AS (PARTITION BY site_tariff, site_id ORDER BY read_at, id), w_all_by_voltage_filled AS (ORDER BY voltage_filled, id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(site_tariff_min_kwh) OVER (PARTITION BY read_at_rank_2, voltage_filled_seq ORDER BY read_at_quintile, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS site_tariff_min_kwh_sum_6_prior,
        SUM(prev_voltage_filled) OVER (ORDER BY voltage ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prev_voltage_filled_sum_3_prior,
        site_tariff_min_kwh - LAG(site_tariff_min_kwh) OVER (PARTITION BY read_at_rank_2, voltage_filled_seq ORDER BY read_at_quintile, id) AS site_tariff_min_kwh_change,
        NTILE(4) OVER w_read_at_rank_2_voltage_fille_2 AS read_at_quintile_quartile,
        ROUND(rows_in_read_at_to_date - AVG(rows_in_read_at_to_date) OVER (PARTITION BY read_at_rank, MONTH(second_read_at)), 2) AS rows_in_read_at_to_date_vs_read_at_rank_second_r
    FROM sequenced
    WINDOW w_read_at_rank_2_voltage_fille_2 AS (PARTITION BY read_at_rank_2, voltage_filled_seq ORDER BY read_at_quintile, id)
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    voltage_rank,
    rows_in_read_at_to_date,
    second_read_at,
    voltage_percent_rank,
    read_at_rank,
    read_at_rank_2,
    read_at_quintile,
    prev_voltage_filled,
    site_tariff_min_kwh_sum_6_prior,
    prev_voltage_filled_sum_3_prior,
    read_at_quintile_quartile,
    rows_in_read_at_to_date_vs_read_at_rank_second_r,
    rows_in_read_at_to_date + voltage_filled_seq AS rows_in_read_at_to_date_plus_voltage_filled_seq
FROM with_windows
ORDER BY rows_in_read_at_to_date
