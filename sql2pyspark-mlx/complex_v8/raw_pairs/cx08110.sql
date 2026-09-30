WITH cleaned_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment
    FROM policyholders
    WHERE joined_on < DATE '2025-07-02'
      AND segment = 'RETAIL'
),
deduped AS (
    -- Keep the latest row per region
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY joined_on DESC, id DESC) AS rn
        FROM cleaned_holders
    ) ranked
    WHERE rn = 1
)
-- Subtotals with GROUPING SETS
SELECT
    region,
    segment,
    GROUPING(region) AS is_total_region,
    COUNT(*) FILTER (WHERE LENGTH(TRIM(holder_name)) >= 6) AS n_length_trim_holder_name_6,
    COUNT(*) AS cnt
FROM deduped
GROUP BY GROUPING SETS ((region, segment), (region), (segment), ())
ORDER BY region NULLS LAST, segment NULLS LAST
