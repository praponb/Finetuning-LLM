WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter
    FROM policies
),
holder_id_stats AS (
    -- Median premium per holder_id
    SELECT holder_id, PERCENTILE_APPROX(premium, 0.5) AS median_premium
    FROM prepared_policys
    GROUP BY holder_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_premium,
        ROUND(s.premium - g.median_premium, 2) AS premium_minus_median
    FROM prepared_policys s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium >= g.median_premium
),
unique_rows AS (
    -- Keep the latest row per status, holder_id
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        start_date_quarter,
        median_premium,
        premium_minus_median
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status, holder_id ORDER BY start_date_quarter DESC, id DESC) AS recency_rank
        FROM above_average
    ) ranked
    WHERE recency_rank = 1
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(c.claimed_amount), 2) FROM claims c WHERE c.policy_id = s.id) AS avg_claim_claimed_amount
    FROM unique_rows s
)
SELECT
    id,
    product,
    premium,
    start_date,
    end_date,
    start_date_quarter,
    premium_minus_median,
    avg_claim_claimed_amount,
    CONCAT_WS(': ', status, product) AS status_product_label
FROM with_stats
ORDER BY premium DESC, id
LIMIT 5
