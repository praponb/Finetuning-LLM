WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        TO_DATE(read_at) AS read_at_date,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE kwh > 20.4
),
rollup_base AS (
    -- Summarise per group
    SELECT
        quality,
        COUNT(*) AS cnt,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        COUNT(DISTINCT site_id) AS distinct_site_ids
    FROM cleaned_readings
    WHERE kwh <= 104.8
    GROUP BY quality
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        GREATEST(cnt, distinct_qualitys) AS greatest_cnt_distinct_qualitys
    FROM rollup_base
    WHERE quality NOT IN ('OK', 'EST')
),
annotated_2 AS (
    -- Label and score rows
    SELECT
        quality,
        distinct_qualitys,
        quality_or_unknown,
        greatest_cnt_distinct_qualitys,
        CASE WHEN distinct_site_ids >= 9 THEN 'HIGH' WHEN distinct_site_ids >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_site_ids_tier,
        LOWER(TRIM(quality)) AS quality_clean
    FROM annotated
),
unique_rows AS (
    -- Keep the latest row per distinct_site_ids_tier, quality_or_unknown
    SELECT
        quality,
        distinct_qualitys,
        quality_or_unknown,
        greatest_cnt_distinct_qualitys,
        distinct_site_ids_tier,
        quality_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY distinct_site_ids_tier, quality_or_unknown ORDER BY greatest_cnt_distinct_qualitys DESC, quality DESC) AS rn
        FROM annotated_2
    ) ranked
    WHERE rn = 1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY greatest_cnt_distinct_qualitys DESC) AS greatest_cnt_distinct_qualitys_dense_rank,
        LEAD(distinct_qualitys, 2, 0) OVER (ORDER BY greatest_cnt_distinct_qualitys DESC, quality) AS next2_distinct_qualitys,
        ROUND(PERCENT_RANK() OVER w_all_by_greatest_cnt_distinct, 3) AS greatest_cnt_distinct_qualitys_percent_rank,
        ROUND(distinct_qualitys - AVG(distinct_qualitys) OVER (PARTITION BY greatest_cnt_distinct_qualitys, quality_or_unknown), 2) AS distinct_qualitys_vs_greatest_cnt_distinct_quali
    FROM unique_rows
    WINDOW w_all_by_greatest_cnt_distinct AS (ORDER BY greatest_cnt_distinct_qualitys DESC)
)
SELECT
    quality,
    distinct_qualitys,
    greatest_cnt_distinct_qualitys,
    distinct_site_ids_tier,
    quality_clean,
    greatest_cnt_distinct_qualitys_dense_rank,
    next2_distinct_qualitys,
    greatest_cnt_distinct_qualitys_percent_rank,
    distinct_qualitys_vs_greatest_cnt_distinct_quali
FROM sequenced
ORDER BY greatest_cnt_distinct_qualitys DESC NULLS FIRST
