WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        LAG(sqft) OVER (PARTITION BY city ORDER BY sqft, id) AS prev_sqft,
        LEAD(sqft) OVER (PARTITION BY city ORDER BY sqft, id) AS next_sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS event_seq
    FROM stores
    WHERE opened_date BETWEEN '2023-03-01' AND '2024-01-31'
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.inventory_last_restock,
        top1.inventory_quantity
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        region,
        sqft,
        prev_sqft,
        next_sqft,
        inventory_last_restock,
        inventory_quantity,
        LEAD(inventory_quantity) OVER w_ordered AS next_inventory_quantity,
        LAG(sqft) OVER w_ordered AS prev_sqft_2,
        LEAD(sqft) OVER w_ordered AS next_sqft_2,
        ROW_NUMBER() OVER w_ordered AS event_seq_2,
        ROW_NUMBER() OVER (PARTITION BY YEAR(opened_date), region ORDER BY event_seq DESC, id) AS row_seq
    FROM with_last
    WINDOW w_ordered AS (PARTITION BY city, region ORDER BY next_sqft, id)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock < s.inventory_last_restock
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        region,
        prev_sqft,
        next_sqft,
        prev_sqft_2,
        next_sqft_2,
        row_seq,
        MAX(prev_sqft_2) OVER w_recent AS running_max_prev_sqft_2,
        MIN(prev_sqft_2) OVER w_recent AS running_min_prev_sqft_2,
        AVG(prev_sqft_2) OVER w_recent AS running_avg_prev_sqft_2,
        COUNT(prev_sqft_2) OVER w_recent AS running_count_prev_sqft_2
    FROM kept
    WINDOW w_recent AS (PARTITION BY region ORDER BY prev_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    prev_sqft,
    next_sqft,
    prev_sqft_2,
    next_sqft_2,
    running_min_prev_sqft_2,
    running_avg_prev_sqft_2,
    running_count_prev_sqft_2
FROM metrics
ORDER BY prev_sqft_2 NULLS FIRST, next_sqft_2 DESC
