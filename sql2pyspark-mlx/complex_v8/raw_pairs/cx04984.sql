WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(product)) AS product_len
    FROM policies
    WHERE product IN ('HOME', 'LIFE')
      AND YEAR(end_date) = 2026
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type IS NOT NULL
              AND TO_DATE(c.reported_at) < DATE '2025-02-18'
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, product ORDER BY start_date NULLS LAST), 3) AS start_date_percent_rank,
        LAG(product_len, 2, 0) OVER w_holder_id_product_by_start_d_2 AS prev2_product_len,
        NTILE(3) OVER (PARTITION BY holder_id, product ORDER BY start_date NULLS LAST, id) AS start_date_tercile,
        LEAD(start_date) OVER (PARTITION BY status ORDER BY start_date NULLS LAST, id) AS next_start_date,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date
    FROM screened
    WINDOW w_holder_id_product_by_start_d_2 AS (PARTITION BY holder_id, product ORDER BY start_date NULLS LAST, id)
)
-- Subtotals with GROUPING SETS
SELECT
    start_date_tercile,
    prev2_product_len,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT status) AS distinct_status
FROM sequenced
GROUP BY GROUPING SETS ((start_date_tercile, prev2_product_len), (start_date_tercile), ())
