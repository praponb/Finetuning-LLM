WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE (status <> 'LAPSED' OR holder_id IS NOT NULL)
),
joined_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.status,
        s.status_slug,
        p.segment AS holder_segment
    FROM filtered_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
filtered AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM joined_holder s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
        )
      AND s.status IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(start_date) OVER w_status_by_start_date AS next_start_date,
        LAST_VALUE(start_date) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        ROUND(CUME_DIST() OVER w_status_by_start_date_ties, 3) AS start_date_cume_dist,
        DATEDIFF(start_date, LAG(start_date) OVER w_status_by_start_date) AS days_since_prev_start_date,
        NTILE(3) OVER w_status_by_start_date AS start_date_tercile
    FROM filtered
    WINDOW w_status_by_start_date AS (PARTITION BY status ORDER BY start_date, id), w_status_by_start_date_ties AS (PARTITION BY status ORDER BY start_date)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(start_date_cume_dist, 0.0) AS start_date_cume_dist_filled,
        COALESCE(holder_segment, 'UNKNOWN') AS holder_segment_or_unknown,
        COALESCE(start_date_cume_dist, 0.0) AS start_date_cume_dist_filled_2
    FROM ranked
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(start_date_cume_dist_filled) OVER (PARTITION BY start_date_tercile, product) AS start_date_tercile_product_sum_start_date_cume_d,
        NTH_VALUE(start_date_tercile, 2) OVER (PARTITION BY holder_id ORDER BY start_date_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date_tercile
    FROM labeled
),
with_parent AS (
    -- Join policyholders
    SELECT s.*, p.joined_on AS holder_joined_on, p.risk_score AS holder_risk_score
    FROM with_trends s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score BETWEEN 0.1 AND 0.7
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    status,
    status_slug,
    holder_segment,
    next_start_date,
    last_start_date,
    start_date_cume_dist,
    days_since_prev_start_date,
    start_date_tercile,
    start_date_cume_dist_filled,
    second_start_date_tercile,
    holder_joined_on,
    holder_risk_score,
    LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
FROM with_parent
