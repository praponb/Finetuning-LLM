WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        UPPER(TRIM(product)) AS product_clean,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label
    FROM policies
    WHERE premium < 3817.5
      AND holder_id IS NOT NULL
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM scoped_policys
    WHERE days_end_date_to_start_date > (SELECT MIN(days_end_date_to_start_date) FROM scoped_policys)
),
holder_id_stats AS (
    -- Avg premium per holder_id
    SELECT holder_id, AVG(premium) AS avg_premium
    FROM with_activity
    GROUP BY holder_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM with_activity s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium > g.avg_premium
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        days_end_date_to_start_date,
        product_clean,
        start_date_label,
        avg_premium,
        premium_minus_avg,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY days_end_date_to_start_date DESC NULLS FIRST, id) AS rn
    FROM above_average
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS last_claim_reported_at,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count_2
    FROM with_windows s
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_count,
        claim_count_2,
        product,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        MIN(rn) AS min_rn
    FROM with_stats
    GROUP BY claim_count, claim_count_2, product
)
SELECT
    claim_count,
    claim_count_2,
    product,
    row_count,
    distinct_ids,
    product_list,
    COALESCE(product, 'UNKNOWN') AS product_or_unknown,
    COALESCE(product, 'UNKNOWN') AS product_or_unknown_2
FROM summary
WHERE product IS NOT NULL
