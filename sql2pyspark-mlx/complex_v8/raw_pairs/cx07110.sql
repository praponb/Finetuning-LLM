WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        LENGTH(TRIM(status)) AS status_len,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE premium > 2013.8
),
latest AS (
    -- Keep the latest row per status
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        status_ref,
        status_len,
        status_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY status_len DESC, id DESC) AS rn
        FROM prepared_policys
    ) ranked
    WHERE rn = 1
)
-- Subtotals with ROLLUP
SELECT
    holder_id,
    status,
    status_len,
    MAX(status_len) AS max_status_len,
    SUM(CASE WHEN product <> 'HOME' THEN status_len ELSE 0 END) AS status_len_product_home,
    ROUND(AVG(status_len), 2) AS avg_status_len
FROM latest
GROUP BY ROLLUP(holder_id, status, status_len)
