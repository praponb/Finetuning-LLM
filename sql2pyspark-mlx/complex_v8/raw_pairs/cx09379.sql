WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM policies
    WHERE end_date BETWEEN DATE '2025-11-20' AND DATE '2027-03-20'
),
unique_rows AS (
    -- Keep the latest row per holder_id, status
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        product_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY premium DESC, id DESC) AS recency_rank
        FROM scoped_policys
    ) ranked
    WHERE recency_rank = 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        product,
        product_or_unknown,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status), 2) AS premium_vs_status_avg,
        ROUND(AVG(premium) OVER w_status, 2) AS status_avg_premium
    FROM unique_rows
    WINDOW w_status AS (PARTITION BY status)
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.approved_amount BETWEEN 1273.7 AND 28426.9
        )
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered
    WHERE status_avg_premium > (SELECT MIN(status_avg_premium) FROM filtered)
),
product_or_unknown_stats AS (
    -- Avg status_avg_premium per product_or_unknown
    SELECT product_or_unknown, AVG(status_avg_premium) AS avg_status_avg_premium
    FROM without_matches
    GROUP BY product_or_unknown
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_status_avg_premium,
        ROUND(s.status_avg_premium - g.avg_status_avg_premium, 2) AS status_avg_premium_minus_avg
    FROM without_matches s
    JOIN product_or_unknown_stats g ON g.product_or_unknown = s.product_or_unknown
    WHERE s.status_avg_premium > g.avg_status_avg_premium
)
SELECT *
FROM (
    SELECT product_or_unknown, product, status_avg_premium
    FROM vs_group
) src
PIVOT (
    SUM(status_avg_premium)
    FOR product IN ('TRAVEL', 'HEALTH', 'HOME')
)
ORDER BY product_or_unknown
