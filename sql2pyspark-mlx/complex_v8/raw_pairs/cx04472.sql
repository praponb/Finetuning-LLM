WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN product IN ('TRAVEL', 'LIFE') THEN 'group_a' WHEN product = 'AUTO' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        product_group,
        LEAD(end_date, 2) OVER w_all_by_end_date AS next2_end_date,
        NTH_VALUE(premium, 2) OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        NTH_VALUE(premium, 2) OVER (ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_2,
        MIN(premium) OVER (PARTITION BY status) AS status_min_premium,
        LAST_VALUE(start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        FIRST_VALUE(start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        COUNT(*) OVER (PARTITION BY status, product) AS rows_in_status_product,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status), 0), 4) AS premium_share_of_status
    FROM valid
    WINDOW w_all_by_end_date AS (ORDER BY end_date, id)
),
tagged AS (
    -- Label and score rows
    SELECT *, COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM windowed
    WHERE next2_end_date < DATE '2026-06-18'
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    end_date,
    status,
    product_group,
    second_premium,
    second_premium_2,
    status_min_premium,
    last_start_date,
    premium_share_of_status,
    status_or_unknown,
    DATE_TRUNC('MONTH', next2_end_date) AS next2_end_date_month
FROM tagged
ORDER BY end_date, id
LIMIT 5
