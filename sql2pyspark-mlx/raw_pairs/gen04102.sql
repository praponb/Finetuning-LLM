WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM orders
    WHERE order_date > DATE '2023-06-09'
      AND channel IS NOT NULL
)
-- Deduplicate with ROW_NUMBER
SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee NULLS FIRST, id) AS latest_rank
FROM base
QUALIFY latest_rank = 1
