WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(country) AS clean_country,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM customers
    WHERE credit_limit > 13900
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        segment,
        resolved_city,
        FIRST_VALUE(age) IGNORE NULLS OVER w_grp AS first_known_age,
        FIRST_VALUE(city, TRUE) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        COUNT(credit_limit) OVER w_ordered AS signup_date_month_count_credit_limit
    FROM cleaned
    WHERE signup_date < '2023-09-28'
    WINDOW w_grp AS (PARTITION BY country, segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY MONTH(signup_date), city)
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_channel
    FROM scored s
    JOIN LATERAL (
            SELECT c.channel AS order_channel
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    -- Set intersection on keys
    SELECT segment
    FROM with_last
    INTERSECT
    SELECT segment
    FROM customers
    WHERE country IN ('US', 'SG', 'TH')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN kept_keys k ON s.segment = k.segment
)
-- Multi-level aggregation
SELECT
    order_channel,
    COUNT(*) FILTER (WHERE segment = 'Corporate') AS corporate_rows,
    SORT_ARRAY(COLLECT_SET(order_channel)) AS order_channel_list
FROM kept_rows
GROUP BY order_channel
ORDER BY corporate_rows DESC NULLS FIRST;
