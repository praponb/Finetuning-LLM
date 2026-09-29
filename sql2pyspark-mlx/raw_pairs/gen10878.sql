WITH lines AS (
    SELECT
        id,
        service,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
cleaned AS (
    SELECT
        *,
        UPPER(key) AS key_upper,
        CASE WHEN status_code >= 444 THEN 'High' WHEN status_code >= 268 THEN 'Medium' ELSE 'Low' END AS status_code_level
    FROM lines
    WHERE key IS NOT NULL
      AND key <> 'region'
),
enriched AS (
    SELECT
        id,
        service,
        key,
        v,
        key_upper,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share_2,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY status_code DESC) AS status_code_dense_rank
    FROM cleaned
),
matched_keys AS (
    SELECT id, service
    FROM enriched
    EXCEPT ALL
    SELECT id, service
    FROM app_logs
    WHERE log_ts >= '2024-06-13'
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id AND s.service = k.service
)
SELECT
    service,
    SUM(CASE WHEN service = 'search' THEN status_code_dense_rank ELSE 0 END) AS search_status_code_dense_rank,
    SUM(CASE WHEN service = 'auth' THEN status_code_share_2 ELSE 0 END) AS auth_status_code_share_2
FROM kept_rows
GROUP BY service
HAVING SUM(CASE WHEN service = 'auth' THEN status_code_share_2 ELSE 0 END) < 1000000
ORDER BY auth_status_code_share_2 DESC NULLS FIRST;
