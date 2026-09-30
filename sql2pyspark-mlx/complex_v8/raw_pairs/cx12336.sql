WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT *
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 10 AND 14
      AND quality = 'BAD'
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        si.commissioned_on AS site_commissioned_on
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(site_commissioned_on) IGNORE NULLS OVER (PARTITION BY site_id, MONTH(site_commissioned_on) ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_site_commissioned_on,
        NTILE(4) OVER (PARTITION BY site_id, quality ORDER BY site_commissioned_on DESC, id) AS site_commissioned_on_quartile,
        NTH_VALUE(site_commissioned_on, 2) OVER (PARTITION BY quality, DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_commissioned_on,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY YEAR(read_at), quality ORDER BY read_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        FIRST_VALUE(read_at) OVER (PARTITION BY site_id, quality ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY site_id, quality ORDER BY site_commissioned_on DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY quality, DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_2,
        LAST_VALUE(site_commissioned_on) OVER (PARTITION BY site_id, quality ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_commissioned_on
    FROM combined_details
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(read_at) ORDER BY second_read_at, id), 3) AS second_read_at_percent_rank,
        SUM(minutes_since_prev_read_at) OVER (PARTITION BY MONTH(first_read_at), site_id) AS first_read_at_month_site_id_sum_minutes_since_pr,
        ROUND(minutes_since_prev_read_at - AVG(minutes_since_prev_read_at) OVER (PARTITION BY MONTH(first_read_at), site_id), 2) AS minutes_since_prev_read_at_vs_first_read_at_mont,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(read_at) ORDER BY second_read_at) AS second_read_at_dense_rank
    FROM ranked
    WINDOW w_first_read_at_month_site_id AS (PARTITION BY MONTH(first_read_at), site_id)
)
SELECT
    id,
    site_id,
    read_at,
    quality,
    site_commissioned_on,
    last_known_site_commissioned_on,
    site_commissioned_on_quartile,
    second_site_commissioned_on,
    second_read_at,
    first_read_at,
    minutes_since_prev_read_at,
    second_read_at_percent_rank,
    first_read_at_month_site_id_sum_minutes_since_pr,
    minutes_since_prev_read_at_vs_first_read_at_mont,
    second_read_at_dense_rank
FROM with_trends
