WITH enriched AS (
    SELECT
        id,
        device_code,
        temp_c,
        status_bits,
        LAG(temp_c) OVER (PARTITION BY device_code ORDER BY status_bits, id) AS prev_temp_c,
        LAG(status_bits) OVER (PARTITION BY device_code ORDER BY status_bits, id) AS prev_status_bits,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) AS seq_no
    FROM device_readings
),
base AS (
    SELECT
        id,
        device_code,
        temp_c,
        prev_temp_c,
        prev_status_bits,
        COALESCE(seq_no, 0) AS seq_no_filled,
        GREATEST(prev_temp_c, prev_status_bits) AS greatest_prev_temp_c_prev_status_bits
    FROM enriched
    WHERE temp_c <= 32
),
ranked AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS LAST, id) = 1
),
scored AS (
    SELECT
        *,
        MIN(seq_no_filled) OVER w_grp AS running_min_seq_no_filled,
        COUNT(seq_no_filled) OVER w_grp AS running_count_seq_no_filled,
        MAX(seq_no_filled) OVER w_grp AS running_max_seq_no_filled,
        LAG(prev_temp_c, 1, 0) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS prev_prev_temp_c
    FROM ranked
    WHERE seq_no_filled <= 48
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY seq_no_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    device_code,
    CASE WHEN running_count_seq_no_filled >= 13 THEN 'HIGH' ELSE 'LOW' END AS running_count_seq_no_filled_tier,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list_2,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events_2,
    GROUPING_ID() AS gid
FROM scored
GROUP BY CUBE (device_code, CASE WHEN running_count_seq_no_filled >= 13 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY running_count_seq_no_filled_tier DESC;
