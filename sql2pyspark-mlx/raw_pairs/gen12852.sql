WITH flattened_attrs AS (
    SELECT
        id,
        service,
        host,
        payload_json,
        context,
        context['version'] AS version,
        COALESCE(context['trace'], 'none') AS trace_or_default,
        SIZE(context) AS num_context,
        COALESCE(context['version'], 'direct') AS version_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
    FROM app_logs
)
SELECT
    *,
    LEAD(duration_sec, 1) OVER w_part AS next_duration_sec,
    DENSE_RANK() OVER w_grp AS num_context_dense_rank
FROM flattened_attrs
WINDOW w_part AS (ORDER BY num_context, id), w_grp AS (ORDER BY num_context DESC NULLS LAST)
