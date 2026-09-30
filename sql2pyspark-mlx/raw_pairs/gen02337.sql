WITH flattened_attrs AS (
    SELECT
        id,
        service,
        host,
        status_code,
        context,
        context['version'] AS version,
        SIZE(context) AS num_context
    FROM app_logs
    WHERE status_code BETWEEN 427 AND 441
)
SELECT
    id,
    service,
    version,
    LEAD(num_context) OVER w_recent AS next_num_context,
    LEAD(status_code) OVER w_recent AS next_status_code,
    ROW_NUMBER() OVER w_recent AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY host ORDER BY num_context DESC, id) AS event_seq
FROM flattened_attrs
WINDOW w_recent AS (PARTITION BY service ORDER BY status_code, id);
