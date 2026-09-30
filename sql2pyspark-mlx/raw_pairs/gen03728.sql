WITH kept AS (
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at >= s.order_date
        )
),
matched_keys AS (
    SELECT status, channel
    FROM kept
    EXCEPT ALL
    SELECT status, channel
    FROM orders
    WHERE YEAR(order_date) >= 2023
),
reconciled AS (
    SELECT s.*
    FROM kept s
    JOIN matched_keys k ON s.status = k.status AND s.channel = k.channel
),
enriched AS (
    SELECT
        src.customer_id,
        src.order_date,
        src.discount,
        d.credit_limit,
        d.country
    FROM reconciled src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
base AS (
    SELECT *, CASE WHEN country IN ('SG', 'JP') THEN 'Tier 1' ELSE 'Other' END AS country_group
    FROM enriched
    WHERE order_date >= DATE '2023-05-21'
      AND discount BETWEEN 0.17 AND 0.2
),
joined AS (
    SELECT
        src.customer_id,
        src.country,
        b.email
    FROM base src
    JOIN customers b
        ON src.customer_id = b.id
),
matched_keys_2 AS (
    SELECT customer_id
    FROM joined
    EXCEPT DISTINCT
    SELECT customer_id
    FROM orders
    WHERE channel != 'partner'
),
kept_rows AS (
    SELECT s.*
    FROM joined s
    JOIN matched_keys_2 k ON s.customer_id = k.customer_id
)
SELECT
    country,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT_IF(country = 'US') AS us_events,
    COUNT(*) AS row_count,
    COUNT_IF(country = 'SG') AS sg_events,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids
FROM kept_rows
GROUP BY 1
ORDER BY row_count DESC
