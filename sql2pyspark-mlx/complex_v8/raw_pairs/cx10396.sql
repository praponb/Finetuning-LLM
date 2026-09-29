WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 1.07, 2) AS kwh_adj
    FROM meter_readings
    WHERE voltage BETWEEN 221.8 AND 233.6
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        SUM(kwh_adj) OVER (PARTITION BY site_id ORDER BY kwh_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh_adj,
        NTILE(3) OVER (ORDER BY kwh ASC, id) AS kwh_tercile,
        NTH_VALUE(voltage, 2) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        LAST_VALUE(kwh_adj) OVER (ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh_adj,
        RANK() OVER w_all_by_kwh_ties AS kwh_rank,
        FIRST_VALUE(kwh_adj) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh_adj,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        RANK() OVER w_all_by_kwh AS kwh_rank_2
    FROM prepared
    WINDOW w_all_by_kwh_ties AS (ORDER BY kwh ASC), w_all_by_kwh AS (ORDER BY kwh ASC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE kwh_rank <= 5
)
SELECT
    id,
    kwh_adj,
    rows_in_site_id,
    running_kwh_adj,
    kwh_tercile,
    second_voltage,
    last_kwh_adj,
    kwh_rank,
    first_known_kwh_adj,
    first_known_kwh,
    kwh_rank_2
FROM top_per_group
