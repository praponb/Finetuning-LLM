WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        attr_key,
        value,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        COALESCE(attr_key, 'NONE') AS attr_key_clean
    FROM flattened
    WHERE created_at < '2023-07-11'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY days_since_created_at DESC, id, attr_key) AS rank_in_group
        FROM filtered
    ) ranked
    WHERE rank_in_group = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        attr_key,
        value,
        days_since_created_at,
        rank_in_group,
        MIN(days_since_created_at) OVER (PARTITION BY customer_id, attr_key ORDER BY rank_in_group, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_days_since_created_at,
        MAX(days_since_created_at) OVER (PARTITION BY customer_id, attr_key ORDER BY rank_in_group, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_days_since_created_at,
        SUM(days_since_created_at) OVER (PARTITION BY customer_id, attr_key ORDER BY rank_in_group, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_days_since_created_at
    FROM deduped
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last3_sum_days_since_created_at >= 350 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_sum_days_since_created_at_tier,
    attr_key,
    COUNT_IF(attr_key = 'weight_kg') AS weight_kg_events,
    PERCENTILE_APPROX(days_since_created_at, ARRAY(0.25, 0.5, 0.75)) AS days_since_created_at_quartiles,
    COUNT_IF(attr_key = 'utm_source') AS utm_source_events
FROM enriched
GROUP BY CASE WHEN last3_sum_days_since_created_at >= 350 THEN 'PREMIUM' ELSE 'STANDARD' END, attr_key
ORDER BY last3_sum_days_since_created_at_tier DESC NULLS FIRST
