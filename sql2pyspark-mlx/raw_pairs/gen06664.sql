WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        CAST(amount AS INT) AS amount_int,
        CONCAT(channel, ' ', status) AS channel_status
    FROM orders
    WHERE customer_id IS NOT NULL
      AND customer_id = 14
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        order_date,
        status,
        discount,
        amount_int,
        MAX(discount) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_discount,
        AVG(discount) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_discount,
        COUNT(discount) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_discount,
        MIN(discount) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_discount,
        PERCENT_RANK() OVER (ORDER BY discount NULLS LAST) AS discount_percent_rank
    FROM valid
    WHERE shipping_fee BETWEEN 19 AND 22
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(discount_percent_rank) OVER w_grp AS last4_sum_discount_percent_rank,
        MAX(discount_percent_rank) OVER w_grp AS last4_max_discount_percent_rank,
        AVG(discount_percent_rank) OVER w_grp AS last4_avg_discount_percent_rank,
        FIRST_VALUE(running_max_discount) IGNORE NULLS OVER (PARTITION BY status ORDER BY discount_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_max_discount
    FROM scored
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id, status
    FROM with_windows
    INTERSECT ALL
    SELECT store_id, status
    FROM orders
    WHERE store_id IN (27, 30, 40)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id AND s.status = k.status
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.unit_price >= 200
        )
),
with_windows_orders AS (
    -- Ranking and gap features
    SELECT
        store_id,
        status,
        amount_int,
        running_max_discount,
        last4_max_discount_percent_rank,
        SUM(discount_percent_rank) OVER w_running AS store_id_sum_discount_percent_rank,
        MIN(discount) OVER w_running AS store_id_min_discount,
        CUME_DIST() OVER (ORDER BY amount_int) AS amount_int_cume_dist
    FROM not_flagged
    WINDOW w_running AS (PARTITION BY store_id)
),
scored_orders AS (
    -- Window metrics over several different partitions
    SELECT
        amount_int,
        running_max_discount,
        store_id_sum_discount_percent_rank,
        amount_int_cume_dist,
        RANK() OVER (PARTITION BY store_id ORDER BY amount_int_cume_dist DESC NULLS LAST) AS amount_int_cume_dist_rank,
        MIN(amount_int) OVER (PARTITION BY status, store_id) AS status_min_amount_int,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY store_id_sum_discount_percent_rank NULLS FIRST) AS store_id_sum_discount_percent_rank_cume_dist
    FROM with_windows_orders
)
SELECT
    amount_int,
    running_max_discount,
    store_id_sum_discount_percent_rank,
    amount_int_cume_dist,
    amount_int_cume_dist_rank,
    status_min_amount_int,
    store_id_sum_discount_percent_rank_cume_dist
FROM scored_orders
ORDER BY amount_int DESC, store_id_sum_discount_percent_rank DESC;
