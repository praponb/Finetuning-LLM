WITH with_last AS (
    SELECT
        s.*,
        latest.subscription_plan,
        latest.subscription_monthly_fee
    FROM customers s
    JOIN LATERAL (
            SELECT c.plan AS subscription_plan, c.monthly_fee AS subscription_monthly_fee
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    SELECT
        *,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_credit_limit,
        CUME_DIST() OVER (ORDER BY credit_limit NULLS FIRST) credit_limit_cume_dist,
        AVG(subscription_monthly_fee) OVER (PARTITION BY subscription_plan ORDER BY credit_limit, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) subscription_monthly_fee_avg_5,
        COUNT(subscription_monthly_fee) OVER (PARTITION BY segment) segment_count_subscription_monthly_fee
    FROM with_last
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY subscription_plan ORDER BY signup_date DESC, id) AS dedup_rank
        FROM scored
    ) ranked
    WHERE dedup_rank = 1
),
filtered AS (
    SELECT
        *,
        credit_limit + credit_limit_cume_dist credit_limit_plus_credit_limit_cume_dist,
        COALESCE(segment, 'N/A') segment_filled
    FROM deduped
    WHERE city IS NOT NULL
),
deduped_customers AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC NULLS LAST, id) = 1
),
deduped_customers_2 AS (
    SELECT *
    FROM deduped_customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY subscription_monthly_fee NULLS LAST, id) = 1
)
SELECT
    *,
    LEAD(subscription_monthly_fee_avg_5, 1, 0) OVER (PARTITION BY country ORDER BY subscription_monthly_fee_avg_5, id) next_subscription_monthly_fee_avg_5,
    NTILE(10) OVER (PARTITION BY city ORDER BY subscription_monthly_fee DESC, id) subscription_monthly_fee_bucket,
    LAST_VALUE(subscription_monthly_fee, TRUE) OVER (PARTITION BY segment, subscription_plan ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_subscription_monthly_fee
FROM deduped_customers_2
