WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE premium >= 2096.6
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.status_slug,
        c.id AS claim_id,
        c.claim_type,
        c.adjuster_id AS claim_adjuster_id
    FROM base_policys s
    JOIN claims c ON c.policy_id = s.id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(premium) OVER (PARTITION BY product, claim_adjuster_id ORDER BY premium, id, claim_id) AS prev_premium,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY claim_adjuster_id)) AS distinct_product_in_claim_adjuster_id,
        ROW_NUMBER() OVER (PARTITION BY product, claim_adjuster_id ORDER BY premium, id, claim_id) AS seq_no,
        LEAD(premium, 3) OVER (PARTITION BY product, claim_adjuster_id ORDER BY premium, id, claim_id) AS next3_premium,
        ROUND(AVG(premium) OVER w_claim_adjuster_id, 2) AS claim_adjuster_id_avg_premium,
        NTILE(10) OVER (PARTITION BY product, claim_adjuster_id ORDER BY premium, id, claim_id) AS premium_decile
    FROM detail_rows_claims
    WINDOW w_claim_adjuster_id AS (PARTITION BY claim_adjuster_id)
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.status_slug,
        s.claim_id,
        s.claim_type,
        s.claim_adjuster_id,
        s.distinct_product_in_claim_adjuster_id,
        s.seq_no,
        s.next3_premium,
        s.premium_decile,
        p.region AS holder_region
    FROM with_trends s
    JOIN policyholders p ON p.id = s.holder_id
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.claim_id,
        s.claim_type,
        s.claim_adjuster_id,
        s.seq_no,
        s.holder_region,
        c.id AS claim_id_2,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at
    FROM joined s
    JOIN claims c ON c.policy_id = s.id
),
claim_status_stats AS (
    -- Avg seq_no per claim_status
    SELECT claim_status, AVG(seq_no) AS avg_seq_no
    FROM line_level
    GROUP BY claim_status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_seq_no, ROUND(s.seq_no - g.avg_seq_no, 2) AS seq_no_minus_avg
    FROM line_level s
    JOIN claim_status_stats g ON g.claim_status = s.claim_status
    WHERE s.seq_no > g.avg_seq_no
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_id_2,
        COUNT(*) FILTER (WHERE holder_region = 'Central') AS n_holder_region_central,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
    FROM outliers
    WHERE seq_no >= 4
    GROUP BY claim_id_2
)
SELECT
    claim_id_2,
    ARRAY_JOIN(ARRAY_SORT(claim_type_list), ',') AS claim_type_list_text
FROM per_group
ORDER BY claim_id_2 DESC
