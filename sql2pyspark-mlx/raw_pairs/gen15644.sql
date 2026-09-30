WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(segment, ' ', city) AS segment_city,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM customers
    WHERE city IS NOT NULL
),
with_last AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.city,
        s.segment,
        s.is_active,
        s.credit_limit,
        s.age,
        s.segment_city,
        x.session_country
    FROM filtered s
    JOIN LATERAL (
            SELECT c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY segment ORDER BY age DESC NULLS LAST) AS rn
        FROM with_last
    ) ranked
    WHERE rn = 1
),
with_last_customers AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.email,
        s.city,
        s.segment,
        s.age,
        s.segment_city,
        s.rn,
        top1.session_device
    FROM deduped s
    JOIN LATERAL (
            SELECT c.device AS session_device
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC NULLS LAST, id) AS rn_2
        FROM with_last_customers
    ) x
    WHERE rn_2 = 1
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            session_device,
            city,
            age
        FROM ranked
    ) src
    PIVOT (
        SUM(age) AS value_sum,
        COUNT(1) AS item_count
        FOR city IN ('Berlin' AS berlin, 'Singapore' AS singapore)
    )
)
SELECT
    session_device,
    berlin_value_sum,
    singapore_value_sum,
    singapore_item_count
FROM by_type
ORDER BY singapore_item_count DESC, session_device;
