WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM policies
    WHERE status NOT IN ('CANCELLED', 'EXPIRED')
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.product_or_unknown,
        p.risk_score AS holder_risk_score
    FROM base s
    JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(holder_risk_score) OVER (PARTITION BY product_or_unknown ORDER BY holder_risk_score, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS holder_risk_score_max_2,
        LAG(holder_risk_score) OVER w_product_or_unknown_by_holder_2 AS prev_holder_risk_score,
        RANK() OVER (ORDER BY holder_risk_score DESC NULLS FIRST) AS holder_risk_score_rank,
        LAG(holder_risk_score) OVER (ORDER BY holder_risk_score DESC NULLS FIRST, id) AS prev_holder_risk_score_2,
        ROW_NUMBER() OVER (PARTITION BY product, holder_id ORDER BY holder_risk_score, id) AS holder_risk_score_seq,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY product), 0), 4) AS holder_risk_score_share_of_product,
        MIN(holder_risk_score) OVER (PARTITION BY product) AS product_min_holder_risk_score,
        MAX(holder_risk_score) OVER (PARTITION BY holder_id ORDER BY holder_risk_score ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS holder_risk_score_max_2_prior,
        SUM(holder_risk_score) OVER (PARTITION BY product) AS product_sum_holder_risk_score,
        FIRST_VALUE(holder_risk_score) OVER (PARTITION BY holder_id ORDER BY holder_risk_score ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_holder_risk_score
    FROM joined
    WINDOW w_product_or_unknown_by_holder_2 AS (PARTITION BY product_or_unknown ORDER BY holder_risk_score, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE holder_risk_score_rank = 1
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_risk_score_rank,
        SUM(CASE WHEN product = 'HEALTH' THEN holder_risk_score_seq ELSE 0 END) AS holder_risk_score_seq_product_health,
        ROUND(SUM(prev_holder_risk_score), 2) AS total_prev_holder_risk_score,
        COUNT(*) AS n_rows
    FROM first_rows
    GROUP BY holder_risk_score_rank
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        holder_risk_score_rank,
        holder_risk_score_seq_product_health,
        total_prev_holder_risk_score,
        n_rows,
        COUNT(*) OVER w_n_rows_holder_risk_score_ran AS rows_in_n_rows_holder_risk_score_rank,
        SUM(n_rows) OVER (PARTITION BY holder_risk_score_rank, n_rows ORDER BY holder_risk_score_seq_product_health ASC, holder_risk_score_rank ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS n_rows_sum_6_prior,
        DENSE_RANK() OVER w_holder_risk_score_rank_n_row_2 AS holder_risk_score_seq_product_health_dense_rank
    FROM grouped
    WINDOW w_n_rows_holder_risk_score_ran AS (PARTITION BY n_rows, holder_risk_score_rank), w_holder_risk_score_rank_n_row_2 AS (PARTITION BY holder_risk_score_rank, n_rows ORDER BY holder_risk_score_seq_product_health ASC, holder_risk_score_rank)
)
SELECT
    holder_risk_score_rank,
    total_prev_holder_risk_score,
    n_rows,
    rows_in_n_rows_holder_risk_score_rank,
    n_rows_sum_6_prior,
    ROUND(n_rows_sum_6_prior / NULLIF(holder_risk_score_rank, 0), 3) AS n_rows_sum_6_prior_per_holder_risk_score_rank
FROM with_windows
ORDER BY rows_in_n_rows_holder_risk_score_rank DESC, n_rows_sum_6_prior DESC
