WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM policies
    WHERE product IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY product, DATE_TRUNC('MONTH', end_date) ORDER BY start_date DESC, id) AS start_date_rank,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY product, DATE_TRUNC('MONTH', end_date) ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product, DATE_TRUNC('MONTH', end_date) ORDER BY start_date DESC, id), 3) AS start_date_percent_rank,
        FIRST_VALUE(start_date) OVER (PARTITION BY product, DATE_TRUNC('MONTH', end_date) ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        DENSE_RANK() OVER (PARTITION BY product, DATE_TRUNC('MONTH', end_date) ORDER BY start_date DESC, id) AS start_date_dense_rank
    FROM prepared
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count
    FROM metrics s
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    end_date,
    status_prefix,
    product_prefix,
    start_date_rank,
    last_known_end_date,
    start_date_percent_rank,
    first_start_date,
    start_date_dense_rank,
    claim_count
FROM profiled
ORDER BY start_date_dense_rank
