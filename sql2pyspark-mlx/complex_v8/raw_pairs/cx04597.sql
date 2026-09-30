WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM policies
    WHERE start_date < DATE '2025-02-14'
      AND holder_id <= 18
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, p.holder_name, p.risk_score AS holder_risk_score
    FROM prepared s
    JOIN policyholders p ON p.id = s.holder_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product ORDER BY holder_risk_score, id) AS holder_risk_score_seq,
        COUNT(*) OVER (PARTITION BY holder_id, product) AS rows_in_holder_id_product,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        LEAD(holder_risk_score, 1, 0) OVER (PARTITION BY holder_id ORDER BY premium DESC, id) AS next_holder_risk_score,
        ROUND(AVG(premium) OVER w_product, 2) AS product_avg_premium,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium DESC), 3) AS premium_percent_rank,
        LEAD(end_date) OVER (ORDER BY premium DESC, id) AS next_end_date,
        RANK() OVER w_all_by_premium_ties AS premium_rank,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        ROUND(CUME_DIST() OVER w_holder_id_by_premium, 3) AS premium_cume_dist,
        RANK() OVER w_holder_id_end_date_month_by AS end_date_rank
    FROM enriched
    WINDOW w_product AS (PARTITION BY product), w_all_by_premium_ties AS (ORDER BY premium DESC), w_holder_id_by_premium AS (PARTITION BY holder_id ORDER BY premium DESC, id), w_holder_id_end_date_month_by AS (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date) ORDER BY end_date, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY premium_share_of_product DESC, id) AS seq_no,
        NTILE(4) OVER (PARTITION BY premium_rank ORDER BY next_end_date ASC, id) AS next_end_date_quartile,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY premium_rank ORDER BY next_end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        SUM(premium_rank) OVER (ORDER BY rows_in_product, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_rank,
        RANK() OVER (ORDER BY rows_in_product) AS rows_in_product_rank
    FROM metrics
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    end_date,
    status_ref,
    holder_name,
    holder_risk_score_seq,
    premium_share_of_product,
    next_holder_risk_score,
    product_avg_premium,
    premium_rank,
    rows_in_product,
    end_date_rank,
    seq_no,
    next_end_date_quartile,
    first_known_end_date,
    running_premium_rank
FROM with_trends
WHERE premium > 1042.5
ORDER BY id, next_end_date_quartile DESC NULLS FIRST
LIMIT 20
