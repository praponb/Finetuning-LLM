WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        DATE_ADD(start_date, 30) AS start_date_plus_30d
    FROM policies
    WHERE start_date < DATE '2025-02-14'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        product,
        premium,
        end_date,
        status,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id, status ORDER BY premium DESC), 3) AS premium_cume_dist,
        ROUND(premium - AVG(premium) OVER w_holder_id, 2) AS premium_vs_holder_id_avg,
        NTILE(10) OVER (PARTITION BY holder_id, status ORDER BY premium DESC, id) AS premium_decile,
        MAX(premium) OVER (PARTITION BY holder_id, status ORDER BY premium DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_max_1_prior
    FROM prepared_policys
    WINDOW w_holder_id AS (PARTITION BY holder_id)
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(product, 'ALL') AS product,
    COALESCE(status, 'ALL') AS status,
    premium_decile,
    MIN(end_date) AS first_end_date,
    COUNT(*) AS row_count
FROM with_windows
GROUP BY GROUPING SETS ((product, status, premium_decile), (product), (status))
ORDER BY product NULLS LAST, status NULLS LAST, premium_decile NULLS LAST
