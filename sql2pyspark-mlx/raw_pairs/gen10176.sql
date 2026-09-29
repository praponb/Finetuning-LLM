WITH base AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATEDIFF(CURRENT_DATE(), start_date) / 30 AS months_since_start_date
    FROM subscriptions
    WHERE end_date >= DATE '2023-06-04'
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY months_since_start_date NULLS LAST, id) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
valid AS (
    SELECT
        id,
        customer_id,
        end_date,
        monthly_fee,
        months_since_start_date,
        CEIL(monthly_fee) AS monthly_fee_whole
    FROM deduped
    WHERE end_date > DATE '2023-09-19'
),
deduped_subscriptions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY months_since_start_date, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
enriched AS (
    SELECT
        id,
        customer_id,
        monthly_fee,
        months_since_start_date,
        rank_in_group,
        LAG(monthly_fee_whole, 1) OVER (PARTITION BY customer_id ORDER BY rank_in_group, id) AS prev_monthly_fee_whole,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY months_since_start_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_5,
        MAX(monthly_fee_whole) OVER (PARTITION BY customer_id) AS customer_id_max_monthly_fee_whole,
        DENSE_RANK() OVER (PARTITION BY YEAR(end_date), customer_id ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_dense_rank,
        SUM(monthly_fee) OVER (ORDER BY monthly_fee_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM deduped_subscriptions
    WHERE customer_id = 2
),
with_windows AS (
    SELECT
        id,
        customer_id,
        prev_monthly_fee_whole,
        monthly_fee_avg_5,
        monthly_fee_dense_rank,
        LEAD(customer_id_max_monthly_fee_whole) OVER w_ordered AS next_customer_id_max_monthly_fee_whole,
        LEAD(monthly_fee_avg_5) OVER w_ordered AS next_monthly_fee_avg_5,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        LAST_VALUE(monthly_fee_dense_rank, TRUE) OVER w_running AS last_known_monthly_fee_dense_rank
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY monthly_fee, id), w_running AS (ORDER BY monthly_fee_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    SELECT
        s.*,
        d.segment,
        d.signup_date
    FROM with_windows s
    LEFT JOIN customers d
        ON s.customer_id = d.id
)
SELECT
    CASE WHEN last_known_monthly_fee_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS last_known_monthly_fee_dense_rank_tier,
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    APPROX_PERCENTILE(last_known_monthly_fee_dense_rank, 0.5) AS last_known_monthly_fee_dense_rank_p50
FROM joined
GROUP BY last_known_monthly_fee_dense_rank_tier, week_start
