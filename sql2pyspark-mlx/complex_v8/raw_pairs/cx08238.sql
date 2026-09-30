WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        MONTH(start_date) AS start_date_month,
        DATE_TRUNC('WEEK', end_date) AS end_date_week
    FROM policies
    WHERE premium > 2111.3
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(start_date_month / NULLIF(SUM(start_date_month) OVER (PARTITION BY product), 0), 4) AS start_date_month_share_of_product,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product
    FROM base_policys
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.holder_name,
        p.segment AS holder_segment,
        p.region AS holder_region
    FROM windowed s
    JOIN policyholders p ON p.id = s.holder_id AND p.segment = 'SME'
    WHERE p.region = 'North'
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS first_claim_date,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS last_claim_reported_at,
        (SELECT ROUND(AVG(c.claimed_amount), 2) FROM claims c WHERE c.policy_id = s.id) AS avg_claim_claimed_amount
    FROM enriched s
),
holder_id_stats AS (
    -- Max start_date_month per holder_id
    SELECT holder_id, MAX(start_date_month) AS max_start_date_month
    FROM profiled
    GROUP BY holder_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_start_date_month,
        ROUND(s.start_date_month - g.max_start_date_month, 2) AS start_date_month_minus_max
    FROM profiled s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.start_date_month = g.max_start_date_month
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.start_date) FROM policies p WHERE p.holder_id = s.holder_id) AS last_policy_start_date
    FROM vs_group s
),
unique_rows AS (
    -- Keep the earliest row per status, holder_region
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        start_date_month,
        end_date_week,
        start_date_month_share_of_product,
        premium_share_of_product,
        rows_in_product,
        holder_name,
        holder_segment,
        holder_region,
        first_claim_date,
        last_claim_reported_at,
        avg_claim_claimed_amount,
        max_start_date_month,
        start_date_month_minus_max,
        last_policy_start_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status, holder_region ORDER BY start_date_month, id) AS rn
        FROM with_stats
    ) ranked
    WHERE rn = 1
)
-- Subtotals with GROUPING SETS
SELECT
    start_date_month,
    COALESCE(status, 'ALL') AS status,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT holder_segment) AS distinct_holder_segments,
    SUM(start_date_month) FILTER (WHERE UPPER(TRIM(holder_name)) LIKE 'K%') AS start_date_month_upper_trim_holder_name_like
FROM unique_rows
GROUP BY GROUPING SETS ((start_date_month, status), (start_date_month), (status))
ORDER BY start_date_month NULLS LAST, status NULLS LAST
