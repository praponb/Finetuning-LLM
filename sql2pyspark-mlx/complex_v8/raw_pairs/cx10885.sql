WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        ABS(premium - 2844.1) AS premium_distance
    FROM policies
    WHERE (holder_id IS NOT NULL OR status = 'ACTIVE')
),
deduped AS (
    -- Keep the latest row per holder_id, status
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        premium_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY start_date DESC, id DESC) AS row_num
        FROM recent
    ) ranked
    WHERE row_num = 1
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MIN(p.end_date) FROM policies p WHERE p.holder_id = s.holder_id) AS first_policy_end_date,
        (SELECT MAX(p.premium) FROM policies p WHERE p.holder_id = s.holder_id) AS max_policy_premium
    FROM deduped s
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(max_policy_premium - 3560.0) AS max_policy_premium_distance,
        CASE WHEN max_policy_premium >= 4280.0 THEN 'HIGH' WHEN max_policy_premium >= 1400.0 THEN 'MEDIUM' ELSE 'LOW' END AS max_policy_premium_tier,
        max_policy_premium - premium_distance AS max_policy_premium_minus_premium_distance
    FROM with_counts
),
holder_id_stats AS (
    -- Avg premium_distance per holder_id
    SELECT holder_id, AVG(premium_distance) AS avg_premium_distance
    FROM labeled
    GROUP BY holder_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium_distance,
        ROUND(s.premium_distance - g.avg_premium_distance, 2) AS premium_distance_minus_avg
    FROM labeled s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium_distance > g.avg_premium_distance
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(max_policy_premium_distance / NULLIF(SUM(max_policy_premium_distance) OVER (PARTITION BY status), 0), 4) AS max_policy_premium_distance_share_of_status,
        SUM(avg_premium_distance) OVER (ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS avg_premium_distance_sum_6_prior,
        max_policy_premium - LAG(max_policy_premium) OVER (PARTITION BY status, max_policy_premium_tier ORDER BY premium_distance_minus_avg DESC, id) AS max_policy_premium_change
    FROM vs_group
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        first_policy_end_date,
        max_policy_premium,
        max_policy_premium_distance,
        avg_premium_distance,
        premium_distance_minus_avg,
        max_policy_premium_distance_share_of_status,
        avg_premium_distance_sum_6_prior,
        max_policy_premium_change,
        ROUND(avg_premium_distance * 0.25, 2) AS avg_premium_distance_adj
    FROM ranked
)
SELECT id, field, value
FROM (
    SELECT id, max_policy_premium_distance_share_of_status, max_policy_premium_change, avg_premium_distance_adj, avg_premium_distance_sum_6_prior
    FROM tagged
) src
UNPIVOT (
    value FOR field IN (max_policy_premium_distance_share_of_status, max_policy_premium_change, avg_premium_distance_adj, avg_premium_distance_sum_6_prior)
) unp
ORDER BY id, field
