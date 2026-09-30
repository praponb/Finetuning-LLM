WITH flattened_attrs AS (
    SELECT
        id,
        log_ts,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
    FROM app_logs
    WHERE TO_DATE(log_ts) >= '2025-06-20'
)
SELECT
    id,
    user_id,
    duration_text,
    tier,
    LEAD(has_fault) OVER (PARTITION BY tier ORDER BY has_fault ASC, id ASC) AS next_has_fault,
    ROW_NUMBER() OVER (PARTITION BY tier ORDER BY has_fault ASC, id ASC) AS seq_no,
    SUM(has_fault) OVER (PARTITION BY tier) AS tier_sum_has_fault
FROM flattened_attrs;
