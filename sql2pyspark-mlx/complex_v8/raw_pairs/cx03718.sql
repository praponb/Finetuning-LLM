WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM policies
    WHERE status = 'EXPIRED'
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        YEAR(end_date) AS end_date_year,
        CONCAT_WS('-', status, product) AS status_product_label
    FROM base
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.status,
        s.product_slug,
        s.end_date_year,
        s.status_product_label,
        c.id AS claim_id,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id
    FROM enriched s
    JOIN claims c ON c.policy_id = s.id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY claim_status, status ORDER BY end_date_year ASC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        DENSE_RANK() OVER (PARTITION BY holder_id, status ORDER BY claim_reported_at, id, claim_id) AS claim_reported_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY claim_reported_at, id, claim_id) AS claim_reported_at_seq
    FROM exploded_detail
)
SELECT
    claim_adjuster_id,
    claim_id,
    end_date_year,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT claim_status) AS distinct_claim_status,
    MAX(claim_reported_at) AS last_claim_reported_at,
    ROUND(STDDEV(claim_claimed_amount), 2) AS stddev_claim_claimed_amount
FROM ranked
GROUP BY ROLLUP(claim_adjuster_id, claim_id, end_date_year)
ORDER BY claim_adjuster_id NULLS LAST, claim_id NULLS LAST, end_date_year NULLS LAST
