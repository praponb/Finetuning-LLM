WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE (read_at < TIMESTAMP '2025-03-02 09:00:00' OR kwh >= 20.4)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_read_at_date_year, 0), 4) AS voltage_share_of_read_at_date_year,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at_date), site_id ORDER BY read_at, id) AS seq_no,
        RANK() OVER w_site_id_by_read_at_date_ties AS read_at_date_rank,
        DENSE_RANK() OVER w_site_id_by_read_at_date AS read_at_date_dense_rank,
        LEAD(voltage) OVER w_read_at_date_month_site_id_b AS next_voltage
    FROM recent_readings
    WINDOW w_read_at_date_year AS (PARTITION BY YEAR(read_at_date)), w_site_id_by_read_at_date_ties AS (PARTITION BY site_id ORDER BY read_at_date DESC), w_site_id_by_read_at_date AS (PARTITION BY site_id ORDER BY read_at_date DESC, id), w_read_at_date_month_site_id_b AS (PARTITION BY DATE_TRUNC('MONTH', read_at_date), site_id ORDER BY read_at, id)
)
SELECT
    seq_no,
    read_at_date_rank,
    read_at_date_dense_rank,
    GROUPING(seq_no) AS is_total_seq_no,
    MAX(read_at_date) AS last_read_at_date,
    SUM(read_at_date_dense_rank) AS total_read_at_date_dense_rank,
    MAX(read_at) AS last_read_at
FROM metrics
GROUP BY GROUPING SETS ((seq_no, read_at_date_rank, read_at_date_dense_rank), (seq_no), ())
ORDER BY seq_no NULLS LAST, read_at_date_rank NULLS LAST, read_at_date_dense_rank NULLS LAST
