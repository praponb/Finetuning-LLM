WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM policies
    WHERE product <> 'LIFE'
      AND premium <= 2945.9
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.end_date_label,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount
    FROM base s
    JOIN claims c ON c.policy_id = s.id AND HOUR(c.reported_at) BETWEEN 10 AND 16
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY holder_id, claim_id ORDER BY claim_date ASC, id, claim_id) AS claim_date_rank,
        ROUND(claim_claimed_amount - AVG(claim_claimed_amount) OVER (PARTITION BY holder_id), 2) AS claim_claimed_amount_vs_holder_id_avg,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY holder_id, claim_id ORDER BY claim_date ASC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        SUM(claim_claimed_amount) OVER (PARTITION BY holder_id, claim_id ORDER BY claim_date ASC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claim_claimed_amount
    FROM line_level
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(claim_date_rank / NULLIF(SUM(claim_date_rank) OVER (PARTITION BY status, product), 0), 4) AS claim_date_rank_share_of_status_product,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY claim_date_rank, product ORDER BY claim_reported_at, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date_2,
        MAX(claim_date_rank) OVER (PARTITION BY status, product) AS status_product_max_claim_date_rank,
        NTILE(10) OVER (PARTITION BY claim_date_rank, product ORDER BY claim_reported_at, id, claim_id) AS claim_reported_at_decile,
        DATEDIFF(first_known_claim_date, LAG(first_known_claim_date) OVER w_claim_date_rank_product_by_c_2) AS days_since_prev_first_known_claim_date
    FROM sequenced
    WINDOW w_claim_date_rank_product_by_c_2 AS (PARTITION BY claim_date_rank, product ORDER BY claim_reported_at, id, claim_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, claim_id, field, metric_value
FROM (
    SELECT id, claim_id, claim_date_rank_share_of_status_product, claim_claimed_amount, claim_claimed_amount_vs_holder_id_avg
    FROM sequenced_2
) src
UNPIVOT (
    metric_value FOR field IN (claim_date_rank_share_of_status_product, claim_claimed_amount, claim_claimed_amount_vs_holder_id_avg)
) unp
