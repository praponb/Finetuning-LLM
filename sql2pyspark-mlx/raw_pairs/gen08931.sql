WITH enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.city,
        b.opened_date
    FROM orders e
    INNER JOIN stores b
        ON e.store_id <=> b.id
),
enriched_orders AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        amount,
        discount,
        city,
        LEAD(discount) OVER w_ordered AS next_discount,
        LAG(discount) OVER w_ordered AS prev_discount,
        LEAD(amount) OVER w_ordered AS next_amount,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY channel, store_id ORDER BY opened_date ASC, id ASC)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY row_seq ASC NULLS LAST, id ASC) AS row_num
    FROM enriched_orders
    QUALIFY row_num = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY prev_discount ASC NULLS LAST, id ASC) AS latest_rank
        FROM deduped
    ) ranked
    WHERE latest_rank = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM latest
    EXCEPT
    SELECT customer_id, id
    FROM orders
    WHERE shipping_fee >= 20
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
SELECT
    id,
    amount,
    discount,
    city,
    prev_discount,
    next_amount,
    row_seq,
    latest_rank,
    CASE WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'EXTENDED' WHEN city RLIKE '^(Bangkok|Singapore)$' THEN 'CORE' ELSE 'GROUP_B' END AS city_zone
FROM reconciled
