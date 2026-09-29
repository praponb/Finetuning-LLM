WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        latency_ms,
        status_code,
        payload_json,
        context['region'] AS region,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
    FROM app_logs
    WHERE latency_ms <= 860
)
-- Running and moving aggregates
SELECT
    *,
    LAG(status_code) OVER w_recent AS prev_status_code,
    LEAD(status_code) OVER w_recent AS next_status_code,
    LEAD(flag_bit1) OVER w_recent AS next_flag_bit1,
    LAG(flag_bit1) OVER w_recent AS prev_flag_bit1,
    ROW_NUMBER() OVER w_recent AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY tier ORDER BY status_code DESC, id) AS row_seq
FROM parsed
WINDOW w_recent AS (PARTITION BY tier ORDER BY flag_bit1, id)
