WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE premium <= 3719.7
      AND holder_id >= 13
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT s.*, p.risk_score AS holder_risk_score
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(premium / NULLIF(SUM(premium) OVER w_end_date_year_status, 0), 4) AS premium_share_of_end_date_year_status,
        COUNT(*) OVER w_holder_id_status AS rows_in_holder_id_status,
        LAST_VALUE(holder_risk_score) IGNORE NULLS OVER (PARTITION BY status_len, product ORDER BY status_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_risk_score,
        LEAD(holder_risk_score) OVER (PARTITION BY status_len ORDER BY end_date, id) AS next_holder_risk_score,
        SUM(status_len) OVER (PARTITION BY YEAR(end_date), status) AS end_date_year_status_sum_status_len,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_id)) AS distinct_product_in_holder_id,
        SUM(premium) OVER w_holder_id AS holder_id_sum_premium,
        LAG(end_date, 3) OVER (PARTITION BY holder_id ORDER BY start_date, id) AS prev3_end_date,
        LAST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        ROUND(status_len / NULLIF(SUM(status_len) OVER (PARTITION BY holder_id, status), 0), 4) AS status_len_share_of_holder_id_status,
        NTH_VALUE(status_len, 2) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_status_len,
        ROW_NUMBER() OVER (PARTITION BY status_len, product ORDER BY status_len, id) AS rn,
        SUM(holder_risk_score) OVER (ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS holder_risk_score_sum_3,
        MIN(holder_risk_score) OVER (PARTITION BY holder_id, status) AS holder_id_status_min_holder_risk_score
    FROM with_parent_holder
    WINDOW w_end_date_year_status AS (PARTITION BY YEAR(end_date), status), w_holder_id_status AS (PARTITION BY holder_id, status), w_holder_id AS (PARTITION BY holder_id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        status_len,
        holder_risk_score,
        premium_share_of_end_date_year_status,
        rows_in_holder_id_status,
        last_known_holder_risk_score,
        distinct_product_in_holder_id,
        prev3_end_date,
        last_known_start_date,
        second_status_len,
        rn,
        holder_risk_score_sum_3,
        holder_id_status_min_holder_risk_score,
        ROUND(status_len / NULLIF(SUM(status_len) OVER w_second_status_len, 0), 4) AS status_len_share_of_second_status_len,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY second_status_len), 0), 4) AS premium_share_of_second_status_len,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date_year_status_sum_status_len DESC) AS end_date_year_status_sum_status_len_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date_year_status_sum_status_len DESC), 3) AS end_date_year_status_sum_status_len_percent_rank
    FROM sequenced
    WINDOW w_second_status_len AS (PARTITION BY second_status_len)
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    end_date,
    status,
    status_len,
    premium_share_of_end_date_year_status,
    rows_in_holder_id_status,
    last_known_holder_risk_score,
    distinct_product_in_holder_id,
    prev3_end_date,
    second_status_len,
    rn,
    holder_risk_score_sum_3,
    status_len_share_of_second_status_len,
    premium_share_of_second_status_len,
    end_date_year_status_sum_status_len_rank,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown,
    ROUND(holder_risk_score_sum_3 * 1.07, 2) AS holder_risk_score_sum_3_adj
FROM windowed
ORDER BY rows_in_holder_id_status DESC NULLS FIRST
