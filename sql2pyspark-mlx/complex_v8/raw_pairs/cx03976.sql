WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM policies
    WHERE end_date >= DATE '2025-10-12'
      AND product = 'TRAVEL'
),
deduped AS (
    -- Keep the latest row per product
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        status_ref,
        end_date_label,
        days_end_date_to_start_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY days_end_date_to_start_date DESC, id DESC) AS recency_rank
        FROM filtered_policys
    ) ranked
    WHERE recency_rank = 1
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date >= DATE '2024-08-18'
        )
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(status, 'ALL') AS status,
    holder_id,
    COALESCE(product, 'ALL') AS product,
    SUM(days_end_date_to_start_date) AS total_days_end_date_to_start_date,
    ROUND(AVG(days_end_date_to_start_date), 2) AS avg_days_end_date_to_start_date
FROM filtered
GROUP BY ROLLUP(status, holder_id, product)
