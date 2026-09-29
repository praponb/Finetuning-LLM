WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT_WS(' / ', product, status) AS product_status_label,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM policies
    WHERE product NOT IN ('TRAVEL', 'HOME')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date DESC), 3) AS start_date_percent_rank,
        SUM(premium) OVER (ORDER BY start_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_sum_3_prior,
        DATEDIFF(start_date, LAG(start_date) OVER (ORDER BY start_date DESC, id)) AS days_since_prev_start_date
    FROM valid
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        status_prefix,
        start_date_percent_rank,
        premium_sum_3_prior,
        days_since_prev_start_date,
        COUNT(*) OVER w_holder_id_product AS rows_in_holder_id_product,
        LAST_VALUE(premium) IGNORE NULLS OVER (ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium
    FROM ranked
    WINDOW w_holder_id_product AS (PARTITION BY holder_id, product)
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS last_claim_reported_at,
        (SELECT MAX(c.claimed_amount) FROM claims c WHERE c.policy_id = s.id) AS max_claim_claimed_amount,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS min_claim_approved_amount
    FROM sequenced s
),
holder_id_stats AS (
    -- Median rows_in_holder_id_product per holder_id
    SELECT holder_id, PERCENTILE_APPROX(rows_in_holder_id_product, 0.5) AS median_rows_in_holder_id_product
    FROM with_stats
    GROUP BY holder_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_rows_in_holder_id_product,
        ROUND(s.rows_in_holder_id_product - g.median_rows_in_holder_id_product, 2) AS rows_in_holder_id_product_minus_median
    FROM with_stats s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.rows_in_holder_id_product >= g.median_rows_in_holder_id_product
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.status_prefix,
        s.start_date_percent_rank,
        s.days_since_prev_start_date,
        s.last_known_premium,
        s.max_claim_claimed_amount,
        s.median_rows_in_holder_id_product,
        s.rows_in_holder_id_product_minus_median,
        c.id AS claim_id,
        c.claim_type,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status
    FROM vs_group s
    JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date_percent_rank,
    max_claim_claimed_amount,
    median_rows_in_holder_id_product,
    rows_in_holder_id_product_minus_median,
    claim_adjuster_id,
    claim_status
FROM with_children_claims
WHERE holder_id IS NOT NULL
ORDER BY rows_in_holder_id_product_minus_median
