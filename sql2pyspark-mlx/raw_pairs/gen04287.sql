WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date,
        CASE WHEN plan = 'standard' THEN 1 ELSE 0 END AS is_standard
    FROM subscriptions
    WHERE monthly_fee > 90
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM base e
    INNER JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date >= sub.start_date
       AND e.start_date < sub.end_date + INTERVAL '14' DAY
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(monthly_fee) OVER (PARTITION BY customer_id, plan ORDER BY subscription_id, id, subscription_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_monthly_fee,
        MIN(monthly_fee) OVER (PARTITION BY customer_id, plan ORDER BY subscription_id, id, subscription_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY customer_id, plan ORDER BY subscription_id, id, subscription_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_monthly_fee
    FROM matched
    WHERE subscription_monthly_fee BETWEEN 210 AND 220
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.age,
        c.credit_limit
    FROM scored e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        is_standard,
        subscription_id,
        subscription_monthly_fee,
        last3_count_monthly_fee,
        last3_min_monthly_fee,
        last3_avg_monthly_fee,
        credit_limit,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) AS monthly_fee_share,
        SUM(last3_count_monthly_fee) OVER (PARTITION BY customer_id ORDER BY subscription_id, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last3_count_monthly_fee
    FROM joined
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY last3_min_monthly_fee NULLS FIRST, id, subscription_id) AS rn
    FROM enriched
    QUALIFY rn = 1
),
enriched_subscriptions AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        subscription_id,
        last3_count_monthly_fee,
        last3_avg_monthly_fee,
        credit_limit,
        cumulative_last3_count_monthly_fee,
        SUM(cumulative_last3_count_monthly_fee) OVER w_ordered AS end_date_year_sum_cumulative_last3_count_monthly_fee,
        AVG(subscription_id) OVER (PARTITION BY status, plan ORDER BY last3_avg_monthly_fee, id, subscription_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS subscription_id_avg_2
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY YEAR(end_date), status)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        subscription_id,
        status,
        last3_avg_monthly_fee,
        credit_limit
    FROM enriched_subscriptions
) src
UNPIVOT (
    val FOR metric IN (last3_avg_monthly_fee, credit_limit)
);
