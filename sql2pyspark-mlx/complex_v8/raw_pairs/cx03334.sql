WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        ADD_MONTHS(start_date, 1) AS start_date_plus_1m
    FROM policies
    WHERE status = 'ACTIVE'
      AND premium BETWEEN 1132.1 AND 4688.7
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.status,
        s.start_date_plus_1m,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM valid s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        start_date_plus_1m,
        holder_region,
        holder_risk_score,
        RANK() OVER (PARTITION BY status, DATE_TRUNC('MONTH', holder_joined_on) ORDER BY holder_joined_on) AS holder_joined_on_rank,
        MAX(premium) OVER (ORDER BY holder_joined_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_max_7,
        DATEDIFF(holder_joined_on, LAG(holder_joined_on) OVER w_all_by_holder_joined_on) AS days_since_prev_holder_joined_on,
        ROUND(CUME_DIST() OVER w_status_holder_joined_on_mont_2, 3) AS holder_joined_on_cume_dist,
        LEAD(start_date_plus_1m) OVER w_status_product_by_holder_ris AS next_start_date_plus_1m,
        LAG(holder_joined_on) OVER (PARTITION BY status, product ORDER BY holder_risk_score ASC, id) AS prev_holder_joined_on,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY holder_region, MONTH(holder_joined_on) ORDER BY start_date_plus_1m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        SUM(premium) OVER (PARTITION BY product, holder_region) AS product_holder_region_sum_premium,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER w_holder_region, 0), 4) AS holder_risk_score_share_of_holder_region
    FROM enriched
    WINDOW w_all_by_holder_joined_on AS (ORDER BY holder_joined_on, id), w_status_holder_joined_on_mont_2 AS (PARTITION BY status, DATE_TRUNC('MONTH', holder_joined_on) ORDER BY holder_joined_on, id), w_status_product_by_holder_ris AS (PARTITION BY status, product ORDER BY holder_risk_score ASC, id), w_holder_region AS (PARTITION BY holder_region)
)
SELECT
    id,
    holder_id,
    product,
    premium,
    status,
    holder_region,
    holder_risk_score,
    holder_joined_on_rank,
    premium_max_7,
    days_since_prev_holder_joined_on,
    next_start_date_plus_1m,
    prev_holder_joined_on,
    last_known_premium,
    holder_risk_score_share_of_holder_region,
    YEAR(start_date_plus_1m) AS start_date_plus_1m_year,
    LEAST(holder_risk_score, holder_joined_on_rank) AS least_holder_risk_score_holder_joined_on_rank
FROM ranked
WHERE holder_risk_score_share_of_holder_region >= 0.5
ORDER BY product DESC, premium_max_7
