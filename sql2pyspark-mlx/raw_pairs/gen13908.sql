WITH joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.end_date,
        e.monthly_fee,
        e.status,
        b.city,
        b.segment,
        b.age
    FROM subscriptions e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM joined a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.end_date >= sub.start_date
       AND a.end_date < sub.end_date + INTERVAL 14 DAYS
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        end_date,
        city,
        subscription_id
    FROM matched
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY subscription_monthly_fee DESC) = 1
),
deduped_subscriptions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY end_date, id, subscription_id) = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(subscription_id) AS subscription_id_whole,
        COALESCE(city, 'NONE') AS resolved_city
    FROM deduped_subscriptions
    WHERE city = 'London'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT DISTINCT
    SELECT id
    FROM subscriptions
    WHERE monthly_fee BETWEEN 70 AND 80
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    city,
    subscription_id,
    subscription_id_whole,
    resolved_city
FROM kept_rows
ORDER BY subscription_id DESC, subscription_id_whole;
