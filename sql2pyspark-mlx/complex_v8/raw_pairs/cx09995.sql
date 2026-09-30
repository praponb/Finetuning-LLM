WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
        CASE WHEN status IN ('ACTIVE', 'LAPSED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE status <> 'ACTIVE'
      AND product = 'HEALTH'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        premium,
        product_prefix,
        status_group,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), status_group ORDER BY premium, id) AS seq_no,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), status_group ORDER BY premium, id) AS premium_quartile,
        LAG(premium) OVER w_end_date_month_status_group AS prev_premium,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), status_group ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date
    FROM filtered_policys
    WINDOW w_end_date_month_status_group AS (PARTITION BY DATE_TRUNC('MONTH', end_date), status_group ORDER BY premium, id)
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.policy_id = s.id) AS min_claim_claimed_amount,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS last_claim_date,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count
    FROM with_trends s
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.product_prefix,
        s.status_group,
        s.seq_no,
        s.premium_quartile,
        s.second_end_date,
        s.min_claim_claimed_amount,
        s.last_claim_date,
        p.region AS holder_region,
        p.holder_name
    FROM with_stats s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM combined_details_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status = 'LAPSED'
              AND YEAR(p.end_date) = 2025
        )
),
latest AS (
    -- Keep the latest row per status_group
    SELECT
        id,
        holder_id,
        premium,
        product_prefix,
        status_group,
        seq_no,
        premium_quartile,
        second_end_date,
        min_claim_claimed_amount,
        last_claim_date,
        holder_region,
        holder_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status_group ORDER BY seq_no DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    holder_id,
    premium,
    product_prefix,
    status_group,
    premium_quartile,
    second_end_date,
    min_claim_claimed_amount,
    last_claim_date,
    holder_region,
    holder_name,
    LOWER(REGEXP_REPLACE(TRIM(holder_name), ' ', '_')) AS holder_name_slug
FROM latest
ORDER BY holder_id NULLS LAST
