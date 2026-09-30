WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 237.5 THEN 'A' WHEN voltage >= 218.4 THEN 'B' ELSE 'C' END AS voltage_tier,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE voltage < 241.8
),
with_parent AS (
    -- Attach site attributes
    SELECT s.*, si.installed_kw AS site_installed_kw, si.tariff AS site_tariff
    FROM cleaned_readings s
    JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at_date,
        site_tariff,
        LEAD(read_at_date) OVER (PARTITION BY site_id, voltage_tier ORDER BY kwh ASC, id) AS next_read_at_date,
        FIRST_VALUE(read_at_date) IGNORE NULLS OVER (PARTITION BY site_tariff, voltage_tier ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at_date,
        DENSE_RANK() OVER (ORDER BY read_at DESC) AS read_at_dense_rank,
        SUM(kwh) OVER (PARTITION BY site_tariff, voltage_tier ORDER BY read_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kwh_sum_3,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_all_by_read_at)) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh, id), 3) AS kwh_percent_rank,
        ROW_NUMBER() OVER (ORDER BY read_at DESC, id) AS read_at_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_tariff, voltage_tier ORDER BY read_at DESC), 3) AS read_at_percent_rank,
        site_installed_kw - LAG(site_installed_kw) OVER w_site_tariff_voltage_tier_by_4 AS site_installed_kw_change,
        COUNT(*) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        RANK() OVER (ORDER BY kwh) AS kwh_rank,
        LAST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        LEAD(kwh) OVER (ORDER BY kwh, id) AS next_kwh,
        site_installed_kw - LAG(site_installed_kw) OVER (ORDER BY kwh, id) AS site_installed_kw_change_2
    FROM with_parent
    WINDOW w_all_by_read_at AS (ORDER BY read_at DESC, id), w_site_tariff_voltage_tier_by_4 AS (PARTITION BY site_tariff, voltage_tier ORDER BY read_at DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at_date,
        site_tariff,
        next_read_at_date,
        first_known_read_at_date,
        kwh_sum_3,
        minutes_since_prev_read_at,
        kwh_percent_rank,
        read_at_seq,
        read_at_percent_rank,
        site_installed_kw_change,
        running_count,
        kwh_rank,
        last_known_read_at,
        next_kwh,
        site_installed_kw_change_2
    FROM windowed
    WHERE read_at_dense_rank = 1
      AND site_id IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        read_at_date,
        site_tariff,
        kwh_sum_3,
        kwh_percent_rank,
        read_at_seq,
        read_at_percent_rank,
        site_installed_kw_change,
        running_count,
        kwh_rank,
        last_known_read_at,
        next_kwh,
        site_installed_kw_change_2,
        ROUND(PERCENT_RANK() OVER w_kwh_rank_by_read_at_percent, 3) AS read_at_percent_rank_percent_rank,
        ROUND(read_at_seq / NULLIF(SUM(read_at_seq) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', next_read_at_date)), 0), 4) AS read_at_seq_share_of_site_id_next_read_at_date_m,
        ROW_NUMBER() OVER (PARTITION BY kwh_rank ORDER BY read_at_percent_rank DESC, id) AS seq_no,
        MIN(read_at_seq) OVER (PARTITION BY site_id ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_read_at_seq_so_far,
        FIRST_VALUE(voltage) OVER (PARTITION BY site_id ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage
    FROM top_rows
    WINDOW w_kwh_rank_by_read_at_percent AS (PARTITION BY kwh_rank ORDER BY read_at_percent_rank DESC)
),
summary AS (
    -- Group-level totals
    SELECT
        read_at_seq,
        CASE WHEN first_voltage >= 238.8 THEN 'large' WHEN first_voltage >= 220.7 THEN 'medium' ELSE 'small' END AS first_voltage_tier,
        site_id,
        PERCENTILE_APPROX(kwh_percent_rank, 0.5) AS median_kwh_percent_rank,
        COUNT(*) FILTER (WHERE site_tariff NOT IN ('EV', 'TOU')) AS n_site_tariff_not_in_ev_tou,
        ROUND(AVG(kwh_percent_rank), 2) AS avg_kwh_percent_rank
    FROM metrics
    GROUP BY read_at_seq, CASE WHEN first_voltage >= 238.8 THEN 'large' WHEN first_voltage >= 220.7 THEN 'medium' ELSE 'small' END, site_id
)
-- Final projection
SELECT
    read_at_seq,
    first_voltage_tier,
    site_id,
    median_kwh_percent_rank,
    n_site_tariff_not_in_ev_tou,
    avg_kwh_percent_rank
FROM summary
ORDER BY avg_kwh_percent_rank DESC, first_voltage_tier DESC NULLS FIRST
