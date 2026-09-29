WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE premium > 2096.6
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(premium / NULLIF(SUM(premium) OVER w_status, 0), 4) AS premium_share_of_status,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product, DATE_TRUNC('MONTH', end_date) ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date
    FROM cleaned
    WINDOW w_status AS (PARTITION BY status)
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    end_date,
    status,
    premium_share_of_status,
    last_known_start_date,
    ABS(premium_share_of_status - 0.6) AS premium_share_of_status_distance,
    COALESCE(product, 'UNKNOWN') AS product_or_unknown
FROM qualified
ORDER BY last_known_start_date, id DESC
