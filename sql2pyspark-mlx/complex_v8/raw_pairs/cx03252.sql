WITH joined_ranges AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
quality_stats AS (
    -- Median rate per quality
    SELECT quality, PERCENTILE_APPROX(rate, 0.5) AS median_rate
    FROM joined_ranges
    GROUP BY quality
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_rate, ROUND(s.rate - g.median_rate, 2) AS rate_minus_median
    FROM joined_ranges s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.rate >= g.median_rate
),
site_id_stats AS (
    -- Median rate_minus_median per site_id
    SELECT site_id, PERCENTILE_APPROX(rate_minus_median, 0.5) AS median_rate_minus_median
    FROM compared
    GROUP BY site_id
),
compared_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_rate_minus_median,
        ROUND(s.rate_minus_median - g.median_rate_minus_median, 2) AS rate_minus_median_minus_median
    FROM compared s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.rate_minus_median >= g.median_rate_minus_median
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, energy_cost, rate
    FROM compared_2
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (energy_cost, rate)
) unp
