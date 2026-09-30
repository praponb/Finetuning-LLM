WITH base_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE YEAR(joined_on) = 2024
      AND region = 'South'
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(holder_name)) AS holder_name_len,
        ABS(risk_score - 0.3) AS risk_score_distance
    FROM base_holders
    WHERE segment <> 'SME'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(segment, 'UNKNOWN') AS segment_or_unknown,
        DAYOFWEEK(joined_on) AS joined_on_dayofweek
    FROM enriched
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.id) AS avg_policy_premium,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.id) AS policy_count
    FROM labeled s
),
latest_per_key AS (
    -- Keep the latest row per segment_or_unknown
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        holder_name_len,
        risk_score_distance,
        segment_or_unknown,
        joined_on_dayofweek,
        avg_policy_premium,
        policy_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY segment_or_unknown ORDER BY policy_count DESC, id DESC) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
),
unique_rows AS (
    -- Keep the latest row per segment_or_unknown
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        holder_name_len,
        risk_score_distance,
        segment_or_unknown,
        joined_on_dayofweek,
        avg_policy_premium,
        policy_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY segment_or_unknown ORDER BY avg_policy_premium DESC, id DESC) AS recency_rank
        FROM latest_per_key
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    holder_name,
    region,
    risk_score,
    joined_on,
    holder_name_len,
    segment_or_unknown,
    policy_count,
    ROUND(holder_name_len / NULLIF(avg_policy_premium, 0), 3) AS holder_name_len_per_avg_policy_premium
FROM unique_rows
ORDER BY id, policy_count DESC
LIMIT 10
