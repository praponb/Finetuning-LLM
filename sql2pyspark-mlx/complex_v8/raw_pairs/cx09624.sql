WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage,
        COALESCE(kwh, 0.0) AS kwh_filled,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
    WHERE kwh < 84.9
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
        LEAST(kwh_filled, kwh) AS least_kwh_filled_kwh
    FROM scoped
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(kwh_filled / NULLIF(SUM(kwh_filled) OVER w_quality_group_quality, 0), 4) AS kwh_filled_share_of_quality_group_quality,
        ROW_NUMBER() OVER (ORDER BY least_kwh_filled_kwh DESC, id) AS least_kwh_filled_kwh_seq,
        ROUND(AVG(kwh_filled) OVER (ORDER BY least_kwh_filled_kwh DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS kwh_filled_avg_1_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY least_kwh_filled_kwh DESC, id), 3) AS least_kwh_filled_kwh_percent_rank,
        COUNT(*) OVER (ORDER BY least_kwh_filled_kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM enriched
    WINDOW w_quality_group_quality AS (PARTITION BY quality_group, quality)
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        kwh,
        quality,
        greatest_kwh_voltage,
        kwh_filled,
        quality_prefix,
        quality_slug,
        least_kwh_filled_kwh,
        kwh_filled_share_of_quality_group_quality,
        least_kwh_filled_kwh_seq,
        kwh_filled_avg_1_prior,
        least_kwh_filled_kwh_percent_rank,
        ROUND(kwh / NULLIF(kwh_filled_avg_1_prior, 0), 3) AS kwh_per_kwh_filled_avg_1_prior
    FROM with_windows
    WHERE quality_group IS NOT NULL
)
SELECT
    id,
    kwh,
    greatest_kwh_voltage,
    kwh_filled,
    quality_slug,
    least_kwh_filled_kwh,
    least_kwh_filled_kwh_seq,
    kwh_filled_avg_1_prior,
    least_kwh_filled_kwh_percent_rank,
    kwh_per_kwh_filled_avg_1_prior,
    SUBSTRING(TRIM(quality_slug), 1, 3) AS quality_slug_prefix
FROM enriched_2
WHERE least_kwh_filled_kwh_percent_rank >= 0.2
