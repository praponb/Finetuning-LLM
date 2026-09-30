WITH prepared AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        CONCAT(holder_name, '#', CAST(id AS STRING)) AS holder_name_ref
    FROM policyholders
    WHERE segment = 'VIP'
      AND risk_score >= 0.2
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM prepared s
    LEFT JOIN policy_stats p ON p.holder_id = s.id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(region), 1, 2) AS region_prefix,
        MONTH(joined_on) AS joined_on_month
    FROM summarized_rows
    WHERE risk_score <= 0.6
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(risk_score * 0.9, 2) AS risk_score_adj,
        ROUND(risk_score * 1.1, 2) AS risk_score_adj_2,
        DATE_FORMAT(joined_on, 'yyyy-MM-dd') AS joined_on_label
    FROM scored
    WHERE policy_count < 7
),
region_stats AS (
    -- Avg policy_count per region
    SELECT region, AVG(policy_count) AS avg_policy_count
    FROM enriched
    GROUP BY region
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_policy_count,
        ROUND(s.policy_count - g.avg_policy_count, 2) AS policy_count_minus_avg
    FROM enriched s
    JOIN region_stats g ON g.region = s.region
    WHERE s.policy_count > g.avg_policy_count
)
SELECT *
FROM (
    SELECT policy_count, region, avg_policy_count
    FROM compared
) src
PIVOT (
    MAX(avg_policy_count)
    FOR region IN ('South', 'North', 'East')
)
