WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE product_id IS NOT NULL
),
joined AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        d.brand,
        d.launch_date,
        d.stock_qty
    FROM valid a
    LEFT JOIN products d
        ON a.product_id = d.id
    WHERE d.stock_qty = 166
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.brand,
        p.channel
    FROM joined e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN channel = 'partner' THEN 1 ELSE 0 END AS is_partner
    FROM enriched
    WHERE brand IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(is_partner) OVER w_ordered AS cumulative_is_partner,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY is_partner DESC, id) AS is_partner_bucket
    FROM base
    WINDOW w_ordered AS (ORDER BY is_partner, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_is_partner >= 399 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_is_partner_tier,
    CASE WHEN is_partner >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_partner_tier,
    channel,
    PERCENTILE_APPROX(is_partner, 0.95) AS is_partner_p95,
    COUNT_IF(channel = 'store') AS store_events,
    ROUND(AVG(cumulative_is_partner), 2) AS avg_cumulative_is_partner,
    COUNT(*) AS num_rows,
    GROUPING(CASE WHEN cumulative_is_partner >= 399 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
FROM with_windows
GROUP BY GROUPING SETS ((CASE WHEN cumulative_is_partner >= 399 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN is_partner >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END, channel), (CASE WHEN cumulative_is_partner >= 399 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN is_partner >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN cumulative_is_partner >= 399 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY is_partner_tier DESC;
