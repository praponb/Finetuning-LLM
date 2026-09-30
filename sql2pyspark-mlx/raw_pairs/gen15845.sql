WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, v
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        k,
        v,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY level), 4) AS status_code_share,
        COUNT(status_code) OVER (PARTITION BY service) AS service_count_status_code,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY status_code DESC) AS status_code_percent_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share_2
    FROM flattened
    WHERE k <> 'trace'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, status_code_share - status_code_share_2 AS status_code_share_minus_status_code_share_2
    FROM enriched
    WHERE k IS NOT NULL
      AND k IS DISTINCT FROM 'trace'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN status_code_share >= 0.84 THEN 'High' WHEN status_code_share >= 0.14 THEN 'Medium' ELSE 'Low' END AS status_code_share_level,
        COALESCE(k, 'DIRECT') AS resolved_k
    FROM filtered
    WHERE k IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        k,
        v,
        service_count_status_code,
        status_code_percent_rank,
        status_code_share_minus_status_code_share_2,
        DENSE_RANK() OVER (ORDER BY status_code_share_minus_status_code_share_2 NULLS LAST) AS status_code_share_minus_status_code_share_2_dense_rank,
        RANK() OVER (ORDER BY service_count_status_code DESC NULLS LAST) AS service_count_status_code_rank,
        AVG(status_code_share_2) OVER (PARTITION BY k) AS k_avg_status_code_share_2
    FROM cleaned
)
SELECT
    service_count_status_code,
    status_code_percent_rank,
    status_code_share_minus_status_code_share_2_dense_rank,
    service_count_status_code_rank,
    k_avg_status_code_share_2
FROM scored
ORDER BY service_count_status_code_rank
