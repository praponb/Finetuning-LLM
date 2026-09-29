WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE premium > 2111.3
      AND start_date >= DATE '2024-10-22'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status_len,
        ROUND(CUME_DIST() OVER w_product_start_date_year_by_s, 3) AS status_len_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(end_date) ORDER BY start_date DESC), 3) AS start_date_cume_dist
    FROM cleaned_policys
    WINDOW w_product_start_date_year_by_s AS (PARTITION BY product, YEAR(start_date) ORDER BY status_len ASC)
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(claimed_amount) AS max_claim_claimed_amount,
        COUNT(*) FILTER (WHERE claim_type NOT IN ('theft', 'medical')) AS claim_claim_type_count,
        MIN(reported_at) AS first_claim_reported_at
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.max_claim_claimed_amount,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count,
        c.first_claim_reported_at
    FROM metrics s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT holder_id, first_claim_reported_at, product, start_date_cume_dist
    FROM profiled
) src
PIVOT (
    MAX(start_date_cume_dist)
    FOR product IN ('AUTO', 'HEALTH', 'HOME')
)
ORDER BY holder_id, first_claim_reported_at
