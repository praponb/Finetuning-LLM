WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len,
        DATE_TRUNC('YEAR', end_date) AS end_date_year,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM policies
    WHERE holder_id >= 3
      AND premium <= 3719.7
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        start_date,
        end_date_year,
        DENSE_RANK() OVER w_status_len_by_end_date_year AS end_date_year_dense_rank,
        ROUND(status_len - AVG(status_len) OVER (PARTITION BY MONTH(end_date)), 2) AS status_len_vs_end_date_month_avg,
        NTILE(10) OVER (ORDER BY start_date, id) AS start_date_decile,
        SUM(status_len) OVER (PARTITION BY MONTH(end_date)) AS end_date_month_sum_status_len,
        LAG(start_date) OVER (ORDER BY start_date, id) AS prev_start_date
    FROM base_policys
    WINDOW w_status_len_by_end_date_year AS (PARTITION BY status_len ORDER BY end_date_year NULLS LAST, id), w_end_date_month AS (PARTITION BY MONTH(end_date))
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count_2,
        (SELECT SUM(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS total_claim_approved_amount
    FROM ranked s
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(start_date) OVER (ORDER BY start_date_decile NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        COUNT(*) OVER (PARTITION BY start_date_decile, claim_count_2) AS rows_in_start_date_decile_claim_count_2,
        NTILE(5) OVER w_claim_count_end_date_year_mo AS end_date_year_quintile,
        ROUND(claim_count_2 / NULLIF(SUM(claim_count_2) OVER (PARTITION BY start_date_decile, claim_count_2), 0), 4) AS claim_count_2_share_of_start_date_decile_claim_c,
        ROUND(CUME_DIST() OVER w_end_date_year_dense_rank_by, 3) AS prev_start_date_cume_dist,
        ROUND(AVG(start_date_decile) OVER (PARTITION BY start_date_decile, claim_count_2), 2) AS start_date_decile_claim_count_2_avg_start_date_d
    FROM with_stats
    WINDOW w_claim_count_end_date_year_mo AS (PARTITION BY claim_count, MONTH(end_date_year) ORDER BY end_date_year DESC, id), w_end_date_year_dense_rank_by AS (PARTITION BY end_date_year_dense_rank ORDER BY prev_start_date, id)
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.policy_id = s.id) AS avg_claim_approved_amount,
        (SELECT COUNT(CASE WHEN c.claim_type IS NOT NULL THEN 1 END) FROM claims c WHERE c.policy_id = s.id) AS claim_claim_type_count,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS min_claim_approved_amount
    FROM with_trends s
)
SELECT
    id,
    start_date,
    end_date_year_dense_rank,
    status_len_vs_end_date_month_avg,
    start_date_decile,
    end_date_month_sum_status_len,
    prev_start_date,
    claim_count_2,
    total_claim_approved_amount,
    last_start_date,
    end_date_year_quintile,
    claim_count_2_share_of_start_date_decile_claim_c,
    prev_start_date_cume_dist,
    start_date_decile_claim_count_2_avg_start_date_d,
    avg_claim_approved_amount,
    claim_claim_type_count,
    min_claim_approved_amount,
    TRUNC(start_date, 'MM') AS start_date_month_start,
    claim_count_2 + end_date_year_dense_rank AS claim_count_2_plus_end_date_year_dense_rank
FROM summarized_rows
WHERE avg_claim_approved_amount > 10000.0
ORDER BY start_date_decile_claim_count_2_avg_start_date_d
