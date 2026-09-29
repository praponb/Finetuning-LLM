WITH exploded AS (
    SELECT
        id,
        service,
        host,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
)
SELECT
    id,
    host,
    status_code,
    k,
    attr_value
FROM exploded
ORDER BY id;
