WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.units >= 23
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft NULLS LAST, id) AS rank_in_group
        FROM eligible
    ) ranked
    WHERE rank_in_group = 1
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.opened_date,
        x.inventory_quantity,
        x.inventory_last_restock
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY inventory_quantity, id) AS seq_no,
        AVG(inventory_quantity) OVER (ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS inventory_quantity_avg_6_prior
    FROM latest_match
    WHERE inventory_last_restock >= '2024-04-01'
)
-- Ranking and gap features
SELECT
    id,
    inventory_last_restock,
    seq_no,
    inventory_quantity_avg_6_prior,
    ROW_NUMBER() OVER (ORDER BY inventory_quantity_avg_6_prior, id) AS row_seq,
    LAST_VALUE(seq_no, TRUE) OVER (ORDER BY inventory_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no
FROM with_windows;
