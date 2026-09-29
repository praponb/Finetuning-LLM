WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-08 12:00:00'
      AND voltage < 233.2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY voltage DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_3,
        NTILE(5) OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY voltage DESC, id) AS voltage_quintile,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id)) AS distinct_quality_in_read_at_month_site_id,
        NTILE(3) OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY voltage DESC, id) AS voltage_tercile,
        ROW_NUMBER() OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY voltage DESC, id) AS voltage_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY voltage DESC, id), 3) AS voltage_cume_dist
    FROM valid_readings
),
summary AS (
    -- Group-level totals
    SELECT
        quality,
        SUM(distinct_quality_in_read_at_month_site_id) FILTER (WHERE quality = 'EST') AS distinct_quality_in_read_at_month_site_id_qualit,
        ROUND(SUM(distinct_quality_in_read_at_month_site_id) / NULLIF(SUM(voltage_cume_dist), 0), 3) AS distinct_quality_in_read_at_month_site_id_to_vol
    FROM with_trends
    WHERE voltage_tercile < 9
    GROUP BY quality
    HAVING SUM(voltage) > 219.5
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        quality,
        distinct_quality_in_read_at_month_site_id_qualit,
        distinct_quality_in_read_at_month_site_id_to_vol,
        ROUND(PERCENT_RANK() OVER (ORDER BY distinct_quality_in_read_at_month_site_id_qualit ASC), 3) AS distinct_quality_in_read_at_month_site_id_qualit_2,
        RANK() OVER (ORDER BY distinct_quality_in_read_at_month_site_id_qualit ASC) AS distinct_quality_in_read_at_month_site_id_qualit_4,
        ROUND(PERCENT_RANK() OVER (ORDER BY distinct_quality_in_read_at_month_site_id_qualit, quality), 3) AS distinct_quality_in_read_at_month_site_id_qualit_5
    FROM summary
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE distinct_quality_in_read_at_month_site_id_qualit_4 > (SELECT AVG(distinct_quality_in_read_at_month_site_id_qualit_4) FROM sequenced)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        quality,
        distinct_quality_in_read_at_month_site_id_qualit,
        distinct_quality_in_read_at_month_site_id_to_vol,
        distinct_quality_in_read_at_month_site_id_qualit_2,
        distinct_quality_in_read_at_month_site_id_qualit_5
    FROM screened
    WHERE distinct_quality_in_read_at_month_site_id_qualit_4 <= 5
      AND distinct_quality_in_read_at_month_site_id_qualit_5 > 0.2
)
SELECT
    quality,
    distinct_quality_in_read_at_month_site_id_qualit,
    distinct_quality_in_read_at_month_site_id_to_vol,
    distinct_quality_in_read_at_month_site_id_qualit_2,
    distinct_quality_in_read_at_month_site_id_qualit_5,
    INITCAP(TRIM(quality)) AS quality_clean,
    INITCAP(TRIM(quality)) AS quality_clean_2
FROM top_rows
ORDER BY distinct_quality_in_read_at_month_site_id_qualit_2 DESC
