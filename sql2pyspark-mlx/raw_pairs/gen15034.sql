WITH valid AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        DATEDIFF(CURRENT_DATE(), paid_at) / 365.25 AS years_since_paid_at,
        TRIM(status) AS clean_status
    FROM payments
    WHERE order_id IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY years_since_paid_at DESC NULLS FIRST, id) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
valid_payments AS (
    SELECT
        *,
        CASE WHEN years_since_paid_at >= 72 THEN 'High' WHEN years_since_paid_at >= 23 THEN 'Medium' ELSE 'Low' END AS years_since_paid_at_tier,
        COALESCE(method, 'UNKNOWN') AS resolved_method
    FROM ranked
    WHERE order_id IS NOT NULL
      AND amount >= 840
),
enriched AS (
    SELECT e.*, b.status
    FROM valid_payments e
    LEFT JOIN orders b
        ON e.order_id <=> b.id
),
metrics AS (
    SELECT
        *,
        LAG(years_since_paid_at, 1) OVER w_grp AS prev_years_since_paid_at,
        LEAD(amount, 1) OVER w_grp_2 AS next_amount,
        PERCENT_RANK() OVER (PARTITION BY method ORDER BY years_since_paid_at) AS years_since_paid_at_percent_rank
    FROM enriched
    WINDOW w_grp AS (PARTITION BY TO_DATE(paid_at), method ORDER BY latest_rank, id), w_grp_2 AS (ORDER BY years_since_paid_at, id)
),
ranked_payments AS (
    SELECT *
    FROM metrics
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY next_amount) = 1
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.years_since_paid_at,
    s.clean_status,
    s.latest_rank,
    s.years_since_paid_at_tier,
    s.resolved_method,
    CASE WHEN method RLIKE '^(card|paypal)$' THEN 'OTHER_ZONE' WHEN method RLIKE '^(bank_transfer|cash)$' THEN 'CORE' ELSE 'PRIORITY' END AS method_zone,
    (SELECT MIN(x.unit_price) FROM order_items x WHERE x.order_id = s.order_id) AS min_order_item_unit_price
FROM ranked_payments s
