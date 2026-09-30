WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN product IN ('HOME', 'AUTO') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date_2
    FROM policies
    WHERE holder_id IS NOT NULL
),
holder_id_stats AS (
    -- Median days_start_date_to_end_date per holder_id
    SELECT holder_id, PERCENTILE_APPROX(days_start_date_to_end_date, 0.5) AS median_days_start_date_to_end_date
    FROM valid_policys
    GROUP BY holder_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_start_date_to_end_date,
        ROUND(s.days_start_date_to_end_date - g.median_days_start_date_to_end_date, 2) AS days_start_date_to_end_date_minus_median
    FROM valid_policys s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.days_start_date_to_end_date >= g.median_days_start_date_to_end_date
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('-', status, product_group) AS status_product_group_label,
        CASE WHEN status IN ('EXPIRED', 'CANCELLED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM outliers
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.product_group,
        s.days_start_date_to_end_date,
        s.days_start_date_to_end_date_2,
        s.days_start_date_to_end_date_minus_median,
        s.status_product_group_label,
        s.status_group,
        s.premium_bucket,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_date
    FROM scored s
    JOIN claims c ON c.policy_id = s.id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        premium,
        status,
        product_group,
        days_start_date_to_end_date,
        days_start_date_to_end_date_2,
        status_product_group_label,
        status_group,
        claim_id,
        claim_adjuster_id,
        claim_date,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label
    FROM with_children_claims
    WHERE status IN ('ACTIVE', 'CANCELLED')
)
SELECT
    id,
    holder_id,
    premium,
    product_group,
    days_start_date_to_end_date,
    days_start_date_to_end_date_2,
    status_product_group_label,
    status_group,
    claim_id,
    claim_adjuster_id,
    claim_date,
    claim_date_label,
    DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label_2,
    ROUND(premium * 1.5, 2) AS premium_adj
FROM tagged
ORDER BY id DESC, premium DESC
