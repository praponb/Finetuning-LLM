WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        YEAR(read_at) AS read_at_year,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
    FROM meter_readings
    WHERE (kwh <= 80.8 OR TO_DATE(read_at) < DATE '2025-03-02')
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM scoped_readings s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
      AND s.site_id IS NOT NULL
),
unique_rows AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at_year,
        read_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh, id) AS recency_rank
        FROM qualified
    ) ranked
    WHERE recency_rank = 1
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_read_at_year_by_read_at_year, 3) AS read_at_year_cume_dist,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_read_at_year_site_id, 0), 4) AS voltage_share_of_read_at_year_site_id,
        LAST_VALUE(read_at) OVER (PARTITION BY YEAR(read_at) ORDER BY read_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        MAX(read_at_year) OVER (PARTITION BY read_at_year, site_id) AS read_at_year_site_id_max_read_at_year
    FROM unique_rows
    WINDOW w_read_at_year_by_read_at_year AS (PARTITION BY YEAR(read_at) ORDER BY read_at_year, id), w_read_at_year_site_id AS (PARTITION BY read_at_year, site_id)
)
-- Subtotals with GROUPING SETS
SELECT
    read_at_year,
    site_id,
    GROUPING(read_at_year) AS is_total_read_at_year,
    MAX(last_read_at) AS last_last_read_at
FROM with_windows
GROUP BY GROUPING SETS ((read_at_year, site_id), (read_at_year), ())
ORDER BY read_at_year NULLS LAST, site_id NULLS LAST
