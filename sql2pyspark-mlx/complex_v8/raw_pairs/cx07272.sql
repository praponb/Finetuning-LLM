WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        ROUND(premium * 0.25, 2) AS premium_adj
    FROM policies
    WHERE holder_id <= 17
),
unique_rows AS (
    -- Keep the latest row per status
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        status_ref,
        premium_adj
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id DESC) AS recency_rank
        FROM recent_policys
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with CUBE
SELECT
    COALESCE(status, 'ALL') AS status,
    holder_id,
    GROUPING(status) AS is_total_status,
    MAX(premium_adj) AS max_premium_adj
FROM unique_rows
GROUP BY CUBE(status, holder_id)
