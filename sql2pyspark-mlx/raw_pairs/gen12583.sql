WITH scored AS (
    SELECT
        id,
        plan,
        monthly_fee,
        COUNT(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_monthly_fee,
        MAX(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY customer_id, status) customer_id_sum_monthly_fee
    FROM subscriptions
),
ranked AS (
    SELECT
        id,
        monthly_fee,
        last3_count_monthly_fee,
        last3_avg_monthly_fee
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY customer_id_sum_monthly_fee, id) = 1
),
base AS (
    SELECT
        *,
        ROUND(last3_avg_monthly_fee, 0) last3_avg_monthly_fee_rounded,
        monthly_fee + last3_avg_monthly_fee monthly_fee_plus_last3_avg_monthly_fee
    FROM ranked
    WHERE monthly_fee BETWEEN 90 AND 150
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY monthly_fee_plus_last3_avg_monthly_fee DESC, id) row_seq,
        ROW_NUMBER() OVER (ORDER BY monthly_fee_plus_last3_avg_monthly_fee DESC, id) row_seq_2,
        AVG(monthly_fee) OVER (ORDER BY last3_count_monthly_fee, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) monthly_fee_avg_3_prior,
        FIRST_VALUE(last3_avg_monthly_fee_rounded, TRUE) OVER (ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_last3_avg_monthly_fee_rounded
    FROM base
),
enriched_subscriptions AS (
    SELECT
        id,
        monthly_fee,
        last3_avg_monthly_fee,
        last3_avg_monthly_fee_rounded,
        monthly_fee_plus_last3_avg_monthly_fee,
        row_seq,
        monthly_fee_avg_3_prior,
        first_known_last3_avg_monthly_fee_rounded,
        MIN(last3_count_monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_last3_count_monthly_fee,
        MAX(last3_count_monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_last3_count_monthly_fee,
        COUNT(last3_count_monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_last3_count_monthly_fee
    FROM enriched
    WHERE monthly_fee_plus_last3_avg_monthly_fee BETWEEN 22 AND 24
),
scored_subscriptions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY monthly_fee_avg_3_prior, id) seq_no,
        LAST_VALUE(running_max_last3_count_monthly_fee) IGNORE NULLS OVER (ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_running_max_last3_count_monthly_fee,
        PERCENT_RANK() OVER (ORDER BY row_seq DESC) row_seq_percent_rank,
        FIRST_VALUE(row_seq) IGNORE NULLS OVER (ORDER BY last3_avg_monthly_fee_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_row_seq
    FROM enriched_subscriptions
),
metrics AS (
    SELECT
        *,
        MIN(monthly_fee_avg_3_prior) OVER w_hist last4_min_monthly_fee_avg_3_prior,
        MAX(monthly_fee_avg_3_prior) OVER w_hist last4_max_monthly_fee_avg_3_prior,
        AVG(monthly_fee_avg_3_prior) OVER w_hist last4_avg_monthly_fee_avg_3_prior,
        SUM(monthly_fee_avg_3_prior) OVER w_hist last4_sum_monthly_fee_avg_3_prior,
        AVG(monthly_fee) OVER (ORDER BY monthly_fee_plus_last3_avg_monthly_fee, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) monthly_fee_avg_3_prior_2
    FROM scored_subscriptions
    WINDOW w_hist AS (ORDER BY first_known_last3_avg_monthly_fee_rounded, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT *
FROM (
    SELECT
        id,
        last3_avg_monthly_fee,
        monthly_fee_plus_last3_avg_monthly_fee,
        first_known_last3_avg_monthly_fee_rounded
    FROM metrics
) src
UNPIVOT INCLUDE NULLS (
    value FOR kpi IN (last3_avg_monthly_fee, monthly_fee_plus_last3_avg_monthly_fee, first_known_last3_avg_monthly_fee_rounded)
);
