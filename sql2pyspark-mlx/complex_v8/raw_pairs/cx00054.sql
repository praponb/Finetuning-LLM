WITH recent AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE LOWER(holder_name) LIKE '%gr%'
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.id) AS policy_count,
        (SELECT MIN(p.end_date) FROM policies p WHERE p.holder_id = s.id) AS first_policy_end_date
    FROM recent s
)
-- Subtotals with GROUPING SETS
SELECT
    policy_count,
    segment,
    GROUPING(policy_count) AS is_total_policy_count,
    COUNT(*) FILTER (WHERE UPPER(TRIM(holder_name)) LIKE 'A%') AS n_upper_trim_holder_name_like,
    ROUND(STDDEV(policy_count), 2) AS stddev_policy_count
FROM summarized_rows
GROUP BY GROUPING SETS ((policy_count, segment), (policy_count), (segment))
