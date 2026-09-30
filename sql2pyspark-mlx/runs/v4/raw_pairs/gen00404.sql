WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        pos,
        item.qty,
        item.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) x AS pos, item
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(qty) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS qty_max_1h,
        NTILE(4) OVER (PARTITION BY customer_id, promo ORDER BY pos, id, pos) AS pos_bucket
    FROM exploded
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY pos DESC) = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM deduped s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.created_at >= sub.start_date
       AND s.created_at <= DATE_ADD(sub.end_date, 7)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        created_at,
        pos,
        qty,
        promo,
        qty_max_1h,
        pos_bucket,
        subscription_id,
        plan,
        monthly_fee,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY pos DESC, id, pos, subscription_id) AS pos_bucket_2,
        AVG(monthly_fee) OVER (ORDER BY created_at, id, pos, subscription_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_6_prior
    FROM with_window
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT customer_id, id
    FROM enriched
    INTERSECT
    SELECT customer_id, id
    FROM carts
    WHERE customer_id IN (11, 25, 26)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('HOUR', created_at) AS hour_window,
        CASE WHEN pos_bucket_2 >= 6 THEN 'HIGH' ELSE 'LOW' END AS pos_bucket_2_tier,
        subscription_id,
        COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
        COUNT(DISTINCT plan) AS distinct_plans,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
        SUM(CASE WHEN promo = 'WELCOME5' THEN qty_max_1h ELSE 0 END) AS welcome5_qty_max_1h,
        COUNT(CASE WHEN promo = 'VIP' THEN 1 END) AS vip_count,
        APPROX_PERCENTILE(pos_bucket_2, 0.95) AS pos_bucket_2_p95
    FROM verified
    GROUP BY DATE_TRUNC('HOUR', created_at), CASE WHEN pos_bucket_2 >= 6 THEN 'HIGH' ELSE 'LOW' END, subscription_id
    HAVING distinct_plans < 1
)
SELECT
    hour_window,
    pos_bucket_2_tier,
    subscription_id,
    welcome5_qty_max_1h,
    vip_count,
    pos_bucket_2_p95
FROM daily
