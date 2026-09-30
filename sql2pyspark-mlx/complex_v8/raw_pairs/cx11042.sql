WITH scoped_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE joined_on BETWEEN DATE '2024-03-22' AND DATE '2025-05-01'
      AND segment = 'RETAIL'
),
unique_rows AS (
    -- Keep the earliest row per region, segment
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY region, segment ORDER BY risk_score, id) AS recency_rank
        FROM scoped_holders
    ) ranked
    WHERE recency_rank = 1
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN region IN ('West', 'East') THEN 'group_a' WHEN region = 'North' THEN 'group_b' ELSE 'other' END AS region_group,
        LENGTH(TRIM(segment)) AS segment_len,
        LENGTH(TRIM(region)) AS region_len
    FROM unique_rows
)
SELECT id, metric_name, reading
FROM (
    SELECT id, segment_len, region_len
    FROM tagged
) src
UNPIVOT (
    reading FOR metric_name IN (segment_len, region_len)
) unp
