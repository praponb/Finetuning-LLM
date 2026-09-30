WITH with_dims AS (
    SELECT
        src.id,
        src.customer_id,
        src.end_date,
        b.segment,
        b.age
    FROM subscriptions src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
filtered AS (
    SELECT
        id,
        customer_id,
        segment,
        DATE_TRUNC('year', end_date) AS end_date_period,
        COALESCE(segment, 'Unknown') AS segment_filled
    FROM with_dims
    WHERE end_date >= TO_DATE('2023-05-18')
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY segment ORDER BY end_date_period DESC) AS latest_rank
        FROM filtered
    ) t
    WHERE latest_rank = 1
),
with_dims_subscriptions AS (
    SELECT
        e.id,
        e.customer_id,
        e.end_date_period,
        e.segment_filled,
        e.latest_rank,
        p.email,
        p.name
    FROM latest e
    JOIN customers p
        ON e.customer_id = p.id
),
matched_keys AS (
    SELECT id, customer_id
    FROM with_dims_subscriptions
    EXCEPT
    SELECT id, customer_id
    FROM subscriptions
    WHERE end_date < '2024-05-12'
),
reconciled AS (
    SELECT s.*
    FROM with_dims_subscriptions s
    JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
)
SELECT
    id,
    customer_id,
    end_date_period,
    latest_rank,
    name
FROM reconciled
ORDER BY customer_id DESC, id DESC;
