WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.5, 2) AS premium_adj,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date
    FROM policies
    WHERE (holder_id >= 18 OR status IN ('EXPIRED', 'ACTIVE', 'CANCELLED'))
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.status,
        s.premium_adj,
        s.days_since_end_date,
        p.risk_score AS holder_risk_score
    FROM recent s
    JOIN policyholders p ON p.id = s.holder_id AND UPPER(TRIM(p.holder_name)) LIKE 'A%'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(premium_adj / NULLIF(SUM(premium_adj) OVER w_product_holder_id, 0), 4) AS premium_adj_share_of_product_holder_id,
        NTH_VALUE(premium_adj, 2) OVER (PARTITION BY status ORDER BY days_since_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_adj,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date DESC, id), 3) AS start_date_percent_rank,
        ROUND(AVG(days_since_end_date) OVER (ORDER BY start_date DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS days_since_end_date_avg_2_prior,
        ROUND(CUME_DIST() OVER (ORDER BY start_date DESC, id), 3) AS start_date_cume_dist,
        LAST_VALUE(premium_adj) IGNORE NULLS OVER (PARTITION BY status ORDER BY days_since_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium_adj,
        SUM(holder_risk_score) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_risk_score,
        premium_adj - LAG(premium_adj) OVER (ORDER BY start_date DESC, id) AS premium_adj_change
    FROM combined_details_holder
    WINDOW w_product_holder_id AS (PARTITION BY product, holder_id)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        status,
        premium_adj,
        days_since_end_date,
        holder_risk_score,
        start_date_percent_rank,
        days_since_end_date_avg_2_prior,
        start_date_cume_dist,
        last_known_premium_adj,
        running_holder_risk_score,
        premium_adj_change,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix
    FROM metrics
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(holder_risk_score, 2) OVER (PARTITION BY status ORDER BY running_holder_risk_score DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_risk_score,
        NTILE(3) OVER w_all_by_running_holder_risk_s AS running_holder_risk_score_tercile,
        ROUND(start_date_cume_dist / NULLIF(SUM(start_date_cume_dist) OVER w_status_holder_id, 0), 4) AS start_date_cume_dist_share_of_status_holder_id,
        MIN(premium_adj_change) OVER (PARTITION BY status, holder_id) AS status_holder_id_min_premium_adj_change,
        ROUND(CUME_DIST() OVER w_status_by_running_holder_ris_2, 3) AS running_holder_risk_score_cume_dist
    FROM labeled
    WINDOW w_all_by_running_holder_risk_s AS (ORDER BY running_holder_risk_score ASC, id), w_status_holder_id AS (PARTITION BY status, holder_id), w_status_by_running_holder_ris_2 AS (PARTITION BY status ORDER BY running_holder_risk_score DESC NULLS FIRST, id)
)
SELECT
    id,
    product,
    status,
    premium_adj,
    days_since_end_date,
    holder_risk_score,
    start_date_percent_rank,
    start_date_cume_dist,
    last_known_premium_adj,
    running_holder_risk_score,
    premium_adj_change,
    product_prefix,
    second_holder_risk_score,
    running_holder_risk_score_cume_dist,
    CONCAT_WS('|', status, product) AS status_product_label,
    COALESCE(product, 'UNKNOWN') AS product_or_unknown
FROM metrics_2
ORDER BY second_holder_risk_score, premium_adj, id
LIMIT 20
