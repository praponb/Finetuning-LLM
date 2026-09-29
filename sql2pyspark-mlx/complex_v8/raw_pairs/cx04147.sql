WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('WEEK', read_at) AS read_at_week,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE (quality = 'OK' OR voltage >= 219.5)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(quality_len) OVER w_read_at_week_month_by_kwh AS next_quality_len,
        ROW_NUMBER() OVER w_read_at_week_month_by_kwh AS rn,
        LAG(voltage, 1, 0) OVER (PARTITION BY MONTH(read_at_week) ORDER BY kwh DESC, id) AS prev_voltage
    FROM recent
    WINDOW w_read_at_week_month_by_kwh AS (PARTITION BY MONTH(read_at_week) ORDER BY kwh DESC, id)
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        kwh,
        voltage,
        quality,
        read_at_week,
        quality_len,
        next_quality_len,
        ROUND(AVG(prev_voltage) OVER (ORDER BY rn ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS prev_voltage_avg_6_prior,
        FIRST_VALUE(next_quality_len) IGNORE NULLS OVER (PARTITION BY rn, quality_len ORDER BY prev_voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_quality_len,
        SIZE(COLLECT_SET(site_id) OVER w_rn) AS distinct_site_id_in_rn,
        ROUND(AVG(kwh) OVER (PARTITION BY rn, quality_len ORDER BY prev_voltage DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_6_prior,
        SUM(next_quality_len) OVER (PARTITION BY rn) AS rn_sum_next_quality_len
    FROM ranked
    WINDOW w_rn AS (PARTITION BY rn)
),
stats AS (
    -- Group-level totals
    SELECT
        quality,
        distinct_site_id_in_rn,
        SUM(CASE WHEN quality = 'OK' THEN 1 ELSE 0 END) AS n_quality_ok,
        COUNT(*) AS cnt,
        MAX(first_known_next_quality_len) AS max_first_known_next_quality_len,
        PERCENTILE_APPROX(distinct_site_id_in_rn, 0.5) AS median_distinct_site_id_in_rn,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM ranked_2
    GROUP BY quality, distinct_site_id_in_rn
),
labeled AS (
    -- Label and score rows
    SELECT
        quality,
        distinct_site_id_in_rn,
        n_quality_ok,
        cnt,
        distinct_qualitys,
        quality_list,
        ROUND(distinct_qualitys / NULLIF(cnt, 0), 3) AS distinct_qualitys_per_cnt,
        ARRAY_JOIN(ARRAY_SORT(quality_list), ',') AS quality_list_text,
        LEAST(distinct_site_id_in_rn, cnt) AS least_distinct_site_id_in_rn_cnt
    FROM stats
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM labeled
    WHERE distinct_qualitys_per_cnt > (SELECT MIN(distinct_qualitys_per_cnt) FROM labeled)
),
flattened AS (
    -- One row per quality_list_item
    SELECT
        s.quality,
        s.distinct_site_id_in_rn,
        s.n_quality_ok,
        s.cnt,
        s.distinct_qualitys,
        s.distinct_qualitys_per_cnt,
        s.quality_list_text,
        s.least_distinct_site_id_in_rn_cnt,
        t.quality_list_item_pos,
        t.quality_list_item
    FROM filtered s
    LATERAL VIEW POSEXPLODE(s.quality_list) t AS quality_list_item_pos, quality_list_item
)
SELECT
    quality,
    distinct_site_id_in_rn,
    cnt,
    distinct_qualitys,
    quality_list_text,
    least_distinct_site_id_in_rn_cnt,
    quality_list_item_pos,
    quality_list_item,
    CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
    LEAST(quality_list_item_pos, distinct_site_id_in_rn) AS least_quality_list_item_pos_distinct_site_id_in_
FROM flattened
ORDER BY quality, distinct_site_id_in_rn, quality_list_item_pos
LIMIT 5
