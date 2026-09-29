WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date
    FROM policies
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status <> 'EXPIRED'
        )
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM without_matches s
    WHERE s.id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.adjuster_id <= 3
              AND c.claim_date >= DATE '2024-08-18'
        )
      AND s.premium < 4678.8
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date,
        c.status AS claim_status,
        c.claim_type
    FROM screened s
    JOIN claims c ON c.policy_id = s.id AND c.claim_date >= DATE '2024-07-22'
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        claim_type,
        ROUND(SUM(claim_claimed_amount), 2) AS total_claim_claimed_amount,
        COUNT(*) AS row_count,
        SUM(claim_claimed_amount) AS total_claim_claimed_amount_2,
        COUNT(DISTINCT id) AS distinct_ids
    FROM detail_rows
    GROUP BY 1, 2, 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        claim_date_month,
        start_date_ym,
        claim_type,
        total_claim_claimed_amount,
        row_count,
        ROUND(row_count / NULLIF(SUM(row_count) OVER (PARTITION BY row_count), 0), 4) AS row_count_share_of_row_count,
        ROUND(total_claim_claimed_amount / NULLIF(SUM(total_claim_claimed_amount) OVER (PARTITION BY row_count), 0), 4) AS total_claim_claimed_amount_share_of_row_count,
        DENSE_RANK() OVER w_distinct_ids_row_count_by_di AS distinct_ids_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY row_count ORDER BY row_count ASC), 3) AS row_count_percent_rank,
        FIRST_VALUE(distinct_ids) OVER (PARTITION BY row_count ORDER BY row_count ASC, claim_date_month, start_date_ym, claim_type ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_ids
    FROM totals
    WINDOW w_distinct_ids_row_count_by_di AS (PARTITION BY distinct_ids, row_count ORDER BY distinct_ids DESC, claim_date_month, start_date_ym, claim_type)
)
SELECT
    claim_date_month,
    claim_type,
    total_claim_claimed_amount,
    row_count,
    row_count_share_of_row_count,
    total_claim_claimed_amount_share_of_row_count,
    distinct_ids_dense_rank,
    row_count_percent_rank,
    DATE_TRUNC('QUARTER', claim_date_month) AS claim_date_month_quarter,
    CASE WHEN claim_type IN ('theft', 'fire') THEN 'group_a' WHEN claim_type = 'delay' THEN 'group_b' ELSE 'other' END AS claim_type_group
FROM ranked
ORDER BY total_claim_claimed_amount DESC
