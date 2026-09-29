WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(shipping_fee, amount) AS least_shipping_fee_amount,
        CASE WHEN channel = 'web' THEN 1 ELSE 0 END AS is_web,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM orders
    WHERE shipping_fee BETWEEN 11 AND 12
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY least_shipping_fee_amount ASC NULLS LAST, id ASC) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY amount DESC, id ASC) AS row_num
    FROM deduped
    QUALIFY row_num = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        status,
        discount,
        shipping_fee,
        least_shipping_fee_amount,
        is_web,
        resolved_channel,
        row_num,
        is_web * shipping_fee AS is_web_times_shipping_fee,
        CASE WHEN least_shipping_fee_amount >= 77 THEN 'High' WHEN least_shipping_fee_amount >= 26 THEN 'Medium' ELSE 'Low' END AS least_shipping_fee_amount_tier,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM latest
    WHERE order_date > DATE '2023-09-12'
      AND customer_id IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status,
        discount,
        shipping_fee,
        resolved_channel,
        row_num,
        least_shipping_fee_amount_tier,
        channel_filled,
        discount + is_web AS discount_plus_is_web
    FROM cleaned
    WHERE store_id IS NOT NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM filtered
    INTERSECT ALL
    SELECT id
    FROM orders
    WHERE discount < 0.12
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.id = k.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.quantity <= 6
        )
)
-- Aggregate with conditional metrics
SELECT
    status,
    COUNT(*) AS row_count,
    ROUND(AVG(discount), 2) AS avg_discount,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count
FROM kept
GROUP BY status
HAVING row_count > 1;
