WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ABS(premium - 2844.1) AS premium_distance,
        QUARTER(end_date) AS end_date_quarter
    FROM policies
    WHERE end_date BETWEEN DATE '2026-01-23' AND DATE '2027-03-20'
      AND status <> 'LAPSED'
),
with_details_holder AS (
    -- Enrich with dimension data
    SELECT s.*, p.segment AS holder_segment, p.risk_score AS holder_risk_score
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        premium_distance,
        end_date_quarter,
        holder_risk_score,
        ROUND(AVG(premium_distance) OVER (PARTITION BY status), 2) AS status_avg_premium_distance,
        NTILE(10) OVER (PARTITION BY product, holder_segment ORDER BY holder_risk_score ASC, id) AS holder_risk_score_decile,
        SIZE(COLLECT_SET(status) OVER w_end_date_year) AS distinct_status_in_end_date_year,
        ROUND(end_date_quarter / NULLIF(SUM(end_date_quarter) OVER w_end_date_year, 0), 4) AS end_date_quarter_share_of_end_date_year,
        LEAD(start_date, 3) OVER w_all_by_premium AS next3_start_date,
        LAST_VALUE(premium_distance) OVER (ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium_distance,
        COUNT(*) OVER (PARTITION BY YEAR(end_date)) AS rows_in_end_date_year,
        LAST_VALUE(premium_distance) OVER (PARTITION BY holder_id ORDER BY start_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium_distance_2,
        MAX(holder_risk_score) OVER (PARTITION BY holder_id ORDER BY start_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_holder_risk_score_so_far,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium DESC NULLS FIRST), 3) AS premium_percent_rank,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER w_end_date_year, 2) AS holder_risk_score_vs_end_date_year_avg,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status), 0), 4) AS premium_share_of_status,
        LAG(end_date) OVER w_holder_id_by_start_date AS prev_end_date,
        ROUND(premium - AVG(premium) OVER (PARTITION BY YEAR(end_date)), 2) AS premium_vs_end_date_year_avg,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY start_date ASC NULLS LAST, id) AS rn
    FROM with_details_holder
    WINDOW w_end_date_year AS (PARTITION BY YEAR(end_date)), w_all_by_premium AS (ORDER BY premium DESC NULLS FIRST, id), w_holder_id_by_start_date AS (PARTITION BY holder_id ORDER BY start_date ASC NULLS LAST, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        product,
        status,
        premium_distance,
        end_date_quarter,
        holder_risk_score,
        status_avg_premium_distance,
        holder_risk_score_decile,
        distinct_status_in_end_date_year,
        rows_in_end_date_year,
        last_premium_distance_2,
        max_holder_risk_score_so_far,
        premium_percent_rank,
        holder_risk_score_vs_end_date_year_avg,
        premium_share_of_status,
        prev_end_date,
        premium_vs_end_date_year_avg,
        rn,
        CONCAT_WS('-', product, status) AS product_status_label,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label
    FROM sequenced
)
SELECT
    id,
    product,
    status,
    premium_distance,
    holder_risk_score,
    status_avg_premium_distance,
    holder_risk_score_decile,
    distinct_status_in_end_date_year,
    rows_in_end_date_year,
    max_holder_risk_score_so_far,
    premium_percent_rank,
    holder_risk_score_vs_end_date_year_avg,
    premium_share_of_status,
    prev_end_date,
    premium_vs_end_date_year_avg,
    rn,
    product_status_label,
    status_prefix,
    end_date_label
FROM derived
ORDER BY premium_distance DESC
