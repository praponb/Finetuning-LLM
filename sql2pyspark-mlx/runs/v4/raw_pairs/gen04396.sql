WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        payload_json,
        context,
        ARRAY_CONTAINS(MAP_KEYS(context), 'version') AS has_version,
        SIZE(context) AS num_context,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        COALESCE(context['region'], 'none') AS region_or_default
    FROM app_logs
)
-- Ranking and gap features
SELECT
    *,
    LEAD(num_context, 2) OVER (PARTITION BY host ORDER BY num_context, id) AS next_num_context,
    NTILE(5) OVER (PARTITION BY host ORDER BY num_context, id) AS num_context_bucket
FROM parsed;
