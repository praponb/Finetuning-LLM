WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        QUARTER(start_date) AS start_date_quarter,
        ABS(premium - 3521.8) AS premium_distance,
        INITCAP(TRIM(status)) AS status_clean
    FROM policies
    WHERE status NOT IN ('CANCELLED', 'LAPSED')
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(approved_amount) AS min_claim_approved_amount,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_approved_amount,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM prepared_policys s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY claim_count ORDER BY min_claim_approved_amount, id)) AS days_since_prev_end_date,
        ROUND(claim_count / NULLIF(SUM(claim_count) OVER (PARTITION BY product), 0), 4) AS claim_count_share_of_product,
        FIRST_VALUE(premium_distance) OVER (PARTITION BY holder_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium_distance,
        NTILE(3) OVER (PARTITION BY holder_id ORDER BY end_date, id) AS end_date_tercile,
        ROUND(start_date_quarter / NULLIF(SUM(start_date_quarter) OVER (PARTITION BY product), 0), 4) AS start_date_quarter_share_of_product
    FROM with_counts
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(first_premium_distance / NULLIF(premium, 0), 3) AS first_premium_distance_per_premium,
        LOWER(TRIM(status_clean)) AS status_clean_clean,
        ROUND(premium_distance / NULLIF(premium, 0), 3) AS premium_distance_per_premium
    FROM metrics
    WHERE first_premium_distance <= 2750.0
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_holder_id_end_date_tercile_b AS claim_count_share_of_product_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_count, YEAR(end_date) ORDER BY claim_count_share_of_product ASC), 3) AS claim_count_share_of_product_cume_dist,
        FIRST_VALUE(premium_distance) IGNORE NULLS OVER (PARTITION BY end_date_tercile ORDER BY premium_distance_per_premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_distance,
        FIRST_VALUE(claim_count_share_of_product) OVER (PARTITION BY end_date_tercile ORDER BY premium_distance_per_premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_count_share_of_product
    FROM labeled
    WINDOW w_holder_id_end_date_tercile_b AS (PARTITION BY holder_id, end_date_tercile ORDER BY claim_count_share_of_product)
),
labeled_2 AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date
    FROM ranked
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    end_date,
    status,
    start_date_quarter,
    premium_distance,
    min_claim_approved_amount,
    claim_count,
    claim_count_share_of_product,
    first_premium_distance,
    end_date_tercile,
    start_date_quarter_share_of_product,
    first_premium_distance_per_premium,
    status_clean_clean,
    premium_distance_per_premium,
    claim_count_share_of_product_rank,
    claim_count_share_of_product_cume_dist,
    first_known_premium_distance,
    first_claim_count_share_of_product,
    days_since_start_date,
    DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label,
    CASE WHEN claim_count >= 6 THEN 'HIGH' WHEN claim_count >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS claim_count_tier
FROM labeled_2
WHERE first_known_premium_distance > 1500.0
ORDER BY premium_distance DESC, min_claim_approved_amount DESC NULLS FIRST
