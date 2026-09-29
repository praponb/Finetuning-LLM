WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROUND(kwh * 1.5, 2) AS kwh_adj,
        read_at + INTERVAL 2 HOURS AS read_at_plus_2h
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-03-02'
      AND voltage > 220.7
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone,
        si.site_name
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(kwh_adj) OVER (ORDER BY site_commissioned_on, id) AS next_kwh_adj,
        kwh - LAG(kwh) OVER (ORDER BY site_commissioned_on, id) AS kwh_change,
        ROUND(kwh_adj - AVG(kwh_adj) OVER w_site_id, 2) AS kwh_adj_vs_site_id_avg,
        ROUND(kwh_adj / NULLIF(SUM(kwh_adj) OVER w_site_id, 0), 4) AS kwh_adj_share_of_site_id,
        SUM(voltage) OVER (ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        LAG(kwh, 1, 0) OVER (PARTITION BY site_id, site_zone ORDER BY read_at DESC NULLS FIRST, id) AS prev_kwh,
        MAX(kwh_adj) OVER (ORDER BY kwh_adj DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_adj_max_4,
        RANK() OVER (PARTITION BY site_id, site_zone ORDER BY read_at DESC NULLS FIRST, id) AS read_at_rank,
        COUNT(*) OVER (ORDER BY kwh_adj DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER w_site_id_site_zone_by_read_at AS read_at_seq
    FROM with_parent_site
    WINDOW w_site_id AS (PARTITION BY site_id), w_site_id_site_zone_by_read_at AS (PARTITION BY site_id, site_zone ORDER BY read_at DESC NULLS FIRST, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        kwh_adj,
        read_at_plus_2h,
        site_commissioned_on,
        site_zone,
        site_name,
        next_kwh_adj,
        kwh_change,
        kwh_adj_vs_site_id_avg,
        kwh_adj_share_of_site_id,
        running_voltage,
        prev_kwh,
        kwh_adj_max_4,
        read_at_rank,
        running_count
    FROM ranked
    WHERE read_at_seq <= 5
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        voltage,
        kwh_adj,
        read_at_plus_2h,
        site_zone,
        site_name,
        next_kwh_adj,
        kwh_change,
        kwh_adj_vs_site_id_avg,
        kwh_adj_share_of_site_id,
        running_voltage,
        prev_kwh,
        kwh_adj_max_4,
        read_at_rank,
        running_count,
        MAX(kwh_change) OVER w_site_zone AS site_zone_max_kwh_change,
        FIRST_VALUE(site_commissioned_on) IGNORE NULLS OVER (PARTITION BY YEAR(read_at_plus_2h), YEAR(read_at) ORDER BY next_kwh_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_commissioned_on,
        LAG(kwh_change, 3) OVER w_read_at_rank_by_next_kwh_adj AS prev3_kwh_change,
        ROW_NUMBER() OVER w_all_by_prev_kwh AS seq_no
    FROM first_rows
    WINDOW w_site_zone AS (PARTITION BY site_zone), w_read_at_rank_by_next_kwh_adj AS (PARTITION BY read_at_rank ORDER BY next_kwh_adj, id), w_all_by_prev_kwh AS (ORDER BY prev_kwh DESC, id)
)
SELECT
    id,
    site_id,
    read_at_plus_2h,
    site_zone,
    site_name,
    kwh_change,
    kwh_adj_vs_site_id_avg,
    prev_kwh,
    kwh_adj_max_4,
    read_at_rank,
    running_count,
    site_zone_max_kwh_change,
    prev3_kwh_change,
    seq_no,
    DATEDIFF(DATE '2026-01-01', read_at_plus_2h) AS days_since_read_at_plus_2h,
    CONCAT(site_zone, '#', CAST(seq_no AS STRING)) AS site_zone_ref
FROM sequenced
ORDER BY site_zone_max_kwh_change, id DESC NULLS FIRST
