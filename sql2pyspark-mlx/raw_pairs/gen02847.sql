WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, value
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level, attr_key ORDER BY status_code DESC, id, attr_key) AS dedup_rank
        FROM lines
    ) t
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        status_code,
        attr_key,
        value,
        dedup_rank,
        SUM(status_code) OVER w_ordered AS last3_sum_status_code,
        MIN(status_code) OVER w_ordered AS last3_min_status_code,
        MAX(status_code) OVER w_ordered AS last3_max_status_code,
        AVG(status_code) OVER w_ordered AS last3_avg_status_code
    FROM latest
    WINDOW w_ordered AS (PARTITION BY level ORDER BY status_code, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
latest_app_logs AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY dedup_rank DESC, id, attr_key) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(last3_min_status_code) IGNORE NULLS OVER w_grp AS last_known_last3_min_status_code,
        ROUND(last3_avg_status_code / SUM(last3_avg_status_code) OVER (PARTITION BY attr_key), 4) AS last3_avg_status_code_share,
        RANK() OVER (ORDER BY last3_min_status_code DESC NULLS LAST) AS last3_min_status_code_rank,
        AVG(last3_avg_status_code) OVER w_part AS last3_avg_status_code_avg_3_prior,
        FIRST_VALUE(last3_avg_status_code, TRUE) OVER w_recent AS first_known_last3_avg_status_code
    FROM latest_app_logs
    WHERE last3_avg_status_code > 400
    WINDOW w_grp AS (ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY attr_key ORDER BY last3_avg_status_code, id, attr_key ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), w_recent AS (PARTITION BY attr_key ORDER BY row_num, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    status_code,
    attr_key,
    dedup_rank,
    last3_min_status_code,
    last3_max_status_code,
    row_num,
    last3_avg_status_code_share,
    last3_min_status_code_rank
FROM scored
ORDER BY status_code
