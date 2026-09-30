WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        ABS(premium - 2289.0) AS premium_distance
    FROM policies
),
enriched_holder AS (
    -- Join policyholders
    SELECT s.*, p.risk_score AS holder_risk_score
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score <= 0.6
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        premium_distance,
        ROUND(AVG(premium_distance) OVER w_holder_id_product_or_unknown, 2) AS holder_id_product_or_unknown_avg_premium_distanc,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY holder_id, product_or_unknown), 0), 4) AS holder_risk_score_share_of_holder_id_product_or_,
        MAX(holder_risk_score) OVER (PARTITION BY status, holder_id) AS status_holder_id_max_holder_risk_score,
        ROW_NUMBER() OVER (ORDER BY end_date DESC, id) AS end_date_seq,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        ROW_NUMBER() OVER w_holder_id_by_premium_distanc AS premium_distance_seq,
        LAST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY holder_risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        NTILE(10) OVER (ORDER BY end_date DESC, id) AS end_date_decile,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY start_date) AS start_date_dense_rank,
        COUNT(*) OVER (PARTITION BY status, holder_id) AS rows_in_status_holder_id,
        MAX(holder_risk_score) OVER (PARTITION BY holder_id, product_or_unknown) AS holder_id_product_or_unknown_max_holder_risk_sco,
        LAG(end_date) OVER (PARTITION BY holder_id ORDER BY premium_distance NULLS LAST, id) AS prev_end_date,
        premium - LAG(premium) OVER w_holder_id_by_premium_distanc AS premium_change
    FROM enriched_holder
    WINDOW w_holder_id_product_or_unknown AS (PARTITION BY holder_id, product_or_unknown), w_holder_id_by_premium_distanc AS (PARTITION BY holder_id ORDER BY premium_distance NULLS LAST, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(premium_distance_seq / NULLIF(SUM(premium_distance_seq) OVER (PARTITION BY end_date_decile), 0), 4) AS premium_distance_seq_share_of_end_date_decile,
        NTH_VALUE(prev_end_date, 2) OVER (PARTITION BY MONTH(last_known_end_date) ORDER BY end_date_seq ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_end_date,
        RANK() OVER (ORDER BY first_known_premium, id) AS first_known_premium_rank,
        end_date_decile - LAG(end_date_decile) OVER (ORDER BY first_known_premium, id) AS end_date_decile_change,
        LAST_VALUE(end_date_seq) IGNORE NULLS OVER (ORDER BY first_known_premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date_seq,
        ROUND(AVG(premium_distance) OVER (PARTITION BY MONTH(last_known_end_date) ORDER BY end_date_seq ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_distance_avg_3
    FROM with_trends
)
SELECT
    id,
    holder_id,
    start_date,
    holder_risk_score_share_of_holder_id_product_or_,
    status_holder_id_max_holder_risk_score,
    end_date_seq,
    first_known_premium,
    premium_distance_seq,
    last_known_end_date,
    end_date_decile,
    start_date_dense_rank,
    holder_id_product_or_unknown_max_holder_risk_sco,
    prev_end_date,
    premium_change,
    second_prev_end_date,
    first_known_premium_rank,
    end_date_decile_change,
    premium_distance_avg_3
FROM windowed
