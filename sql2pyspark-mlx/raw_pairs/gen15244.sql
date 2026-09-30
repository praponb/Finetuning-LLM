WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.order_date,
        a.channel,
        a.amount,
        p.region,
        p.store_name
    FROM orders a
    LEFT JOIN stores p
        ON a.store_id <=> p.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(amount) OVER w_grp AS last7_count_amount,
        MIN(amount) OVER w_grp AS last7_min_amount,
        MAX(amount) OVER w_grp AS last7_max_amount,
        AVG(amount) OVER w_grp AS last7_avg_amount
    FROM with_dims
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY amount ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY store_id ORDER BY amount ASC, id ASC) AS amount_bucket,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY last7_max_amount DESC, id ASC) AS last7_max_amount_bucket
    FROM enriched
    WHERE store_name IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT store_id, channel
    FROM metrics
    EXCEPT
    SELECT store_id, channel
    FROM orders
    WHERE amount IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.store_id = k.store_id AND s.channel = k.channel
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        store_id,
        amount,
        last7_min_amount,
        last7_avg_amount
    FROM reconciled
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (amount, last7_min_amount, last7_avg_amount)
)
