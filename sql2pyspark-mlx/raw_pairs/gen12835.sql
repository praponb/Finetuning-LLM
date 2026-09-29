WITH exploded AS (
    SELECT
        id,
        host,
        log_ts,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
),
parsed AS (
    SELECT
        id,
        status_code,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS is_flagged
    FROM exploded
    WHERE k = 'region'
),
with_windows AS (
    SELECT
        *,
        AVG(status_code) OVER w_grp status_code_avg_6_prior,
        NTILE(10) OVER (PARTITION BY k ORDER BY status_code_high ASC, id ASC, k ASC) status_code_high_bucket,
        RANK() OVER (PARTITION BY k ORDER BY status_code_high DESC NULLS LAST) status_code_high_rank
    FROM parsed
    WHERE k IN ('version', 'trace')
    WINDOW w_grp AS (PARTITION BY k ORDER BY is_flagged ASC, id ASC, k ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
deduped AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code_high_rank DESC, id ASC, k ASC) = 1
)
SELECT
    k,
    SIZE(COLLECT_SET(k)) AS num_ks,
    SUM(CASE WHEN k = 'region' THEN status_code_high_bucket ELSE 0 END) AS region_status_code_high_bucket,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list
FROM deduped
GROUP BY k
ORDER BY k DESC
