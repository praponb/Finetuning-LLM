WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE start_date IS NOT NULL
      AND premium BETWEEN 2096.6 AND 4678.8
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(premium) OVER (PARTITION BY YEAR(start_date) ORDER BY premium_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        MAX(premium) OVER (PARTITION BY YEAR(start_date) ORDER BY premium_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_max_6_prior,
        LAG(start_date) OVER (PARTITION BY YEAR(start_date) ORDER BY premium_bucket, id) AS prev_start_date,
        ROUND(AVG(premium_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date)), 2) AS start_date_month_avg_premium_bucket
    FROM scoped_policys
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.premium_bucket,
        s.running_premium,
        s.premium_max_6_prior,
        s.start_date_month_avg_premium_bucket,
        p.segment AS holder_segment
    FROM metrics s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT SUM(c.claimed_amount) FROM claims c WHERE c.policy_id = s.id) AS total_claim_claimed_amount,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count_2
    FROM joined s
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium_bucket,
        s.premium_max_6_prior,
        s.total_claim_claimed_amount,
        s.claim_count,
        s.claim_count_2,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment_2,
        p.region AS holder_region
    FROM profiled s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
latest_per_key AS (
    -- Keep the latest row per holder_id, holder_segment_2
    SELECT
        id,
        holder_id,
        premium_bucket,
        premium_max_6_prior,
        total_claim_claimed_amount,
        claim_count,
        claim_count_2,
        holder_joined_on,
        holder_segment_2,
        holder_region
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, holder_segment_2 ORDER BY claim_count_2 DESC, id DESC) AS row_num
        FROM joined_holder
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    holder_id,
    premium_max_6_prior,
    total_claim_claimed_amount,
    claim_count,
    claim_count_2,
    holder_joined_on,
    holder_segment_2,
    CONCAT(holder_region, '#', CAST(claim_count_2 AS STRING)) AS holder_region_ref
FROM latest_per_key
