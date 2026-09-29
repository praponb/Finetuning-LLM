WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        read_at + INTERVAL 30 MINUTES AS read_at_plus_30m,
        CASE WHEN voltage >= 233.8 THEN 'large' WHEN voltage >= 216.8 THEN 'medium' ELSE 'small' END AS voltage_tier
    FROM valid
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        quality,
        ROUND(100 * COUNT(CASE WHEN quality NOT IN ('BAD', 'EST') THEN 1 END) / COUNT(*), 1) AS pct_quality_not_in_bad_est,
        ROUND(SUM(voltage) / NULLIF(SUM(quality_len), 0), 3) AS voltage_to_quality_len_ratio,
        COUNT(*) FILTER (WHERE quality = 'OK') AS n_quality_ok,
        COUNT(*) AS cnt
    FROM enriched
    WHERE read_at_plus_30m >= TIMESTAMP '2025-01-25 09:00:00'
    GROUP BY 1, 2
    HAVING COUNT(*) >= 3
),
scored AS (
    -- Label and score rows
    SELECT *, ABS(n_quality_ok - 6) AS n_quality_ok_distance
    FROM grouped
    WHERE pct_quality_not_in_bad_est > 25.0
),
tagged AS (
    -- Add calculated columns
    SELECT
        read_at_month,
        quality,
        voltage_to_quality_len_ratio,
        n_quality_ok,
        n_quality_ok_distance,
        CASE WHEN cnt >= 8 THEN 'large' WHEN cnt >= 6 THEN 'medium' ELSE 'small' END AS cnt_tier
    FROM scored
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE n_quality_ok_distance > (SELECT AVG(n_quality_ok_distance) FROM tagged)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(n_quality_ok_distance) OVER (PARTITION BY n_quality_ok, quality ORDER BY n_quality_ok_distance, read_at_month, quality ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS n_quality_ok_distance_avg_6_prior,
        ROUND((UNIX_TIMESTAMP(read_at_month) - UNIX_TIMESTAMP(LAG(read_at_month) OVER (PARTITION BY n_quality_ok_distance ORDER BY voltage_to_quality_len_ratio, read_at_month, quality))) / 60, 1) AS minutes_since_prev_read_at_month,
        ROUND(voltage_to_quality_len_ratio / NULLIF(SUM(voltage_to_quality_len_ratio) OVER w_cnt_tier_n_quality_ok_distan, 0), 4) AS voltage_to_quality_len_ratio_share_of_cnt_tier_n,
        LEAD(voltage_to_quality_len_ratio, 3, 0) OVER (PARTITION BY n_quality_ok_distance ORDER BY voltage_to_quality_len_ratio, read_at_month, quality) AS next3_voltage_to_quality_len_ratio
    FROM filtered
    WINDOW w_cnt_tier_n_quality_ok_distan AS (PARTITION BY cnt_tier, n_quality_ok_distance), w_n_quality_ok_distance_by_vol AS (PARTITION BY n_quality_ok_distance ORDER BY voltage_to_quality_len_ratio, read_at_month, quality)
)
SELECT
    read_at_month,
    n_quality_ok,
    n_quality_ok_distance,
    n_quality_ok_distance_avg_6_prior,
    minutes_since_prev_read_at_month,
    next3_voltage_to_quality_len_ratio,
    LOWER(TRIM(cnt_tier)) AS cnt_tier_clean,
    LEAST(n_quality_ok_distance, n_quality_ok) AS least_n_quality_ok_distance_n_quality_ok
FROM windowed
ORDER BY cnt_tier_clean DESC, next3_voltage_to_quality_len_ratio NULLS LAST
