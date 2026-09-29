WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(product)) AS product_len,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE holder_id IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(premium, 0.0) AS premium_filled_2,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date,
        DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label
    FROM filtered
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(days_since_end_date - AVG(days_since_end_date) OVER (PARTITION BY holder_id, product), 2) AS days_since_end_date_vs_holder_id_product_avg,
        LAST_VALUE(product_len) OVER (PARTITION BY holder_id ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_product_len,
        SUM(product_len) OVER (PARTITION BY holder_id ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_product_len,
        RANK() OVER (ORDER BY premium_filled_2 DESC, id) AS premium_filled_2_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium_filled_2 DESC), 3) AS premium_filled_2_percent_rank
    FROM tagged
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        SUM(approved_amount) AS total_claim_approved_amount,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_approved_amount,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM metrics s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
latest_per_key AS (
    -- Keep the latest row per holder_id, product
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        product_len,
        premium_filled,
        premium_filled_2,
        days_since_end_date,
        end_date_label,
        days_since_end_date_vs_holder_id_product_avg,
        last_product_len,
        running_product_len,
        premium_filled_2_rank,
        premium_filled_2_percent_rank,
        total_claim_approved_amount,
        claim_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, product ORDER BY premium_filled_2_rank DESC, id DESC) AS rn
        FROM summarized_rows
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    premium,
    start_date,
    end_date,
    product_len,
    premium_filled,
    days_since_end_date,
    premium_filled_2_rank,
    premium_filled_2_percent_rank,
    total_claim_approved_amount,
    claim_count
FROM latest_per_key
ORDER BY premium_filled_2_percent_rank, product_len DESC
