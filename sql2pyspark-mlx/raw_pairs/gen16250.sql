WITH with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.opened_date,
        x.last_restock
    FROM stores s
    JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    opened_date,
    last_restock
FROM with_last
ORDER BY id
LIMIT 5;
