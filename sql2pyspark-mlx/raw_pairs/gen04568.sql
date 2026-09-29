WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS rn
        FROM inventory
    ) ranked
    WHERE rn = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, quantity * rn AS quantity_times_rn
    FROM latest
    WHERE product_id IN (3, 4, 5, 8)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(quantity_times_rn) OVER w_recent AS prev_quantity_times_rn,
        LEAD(quantity_times_rn) OVER w_recent AS next_quantity_times_rn,
        LAG(quantity) OVER w_recent AS prev_quantity,
        LEAD(quantity) OVER w_recent AS next_quantity,
        ROW_NUMBER() OVER w_recent AS row_seq,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY rn, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY quantity, id)
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.*,
        d.opened_date,
        d.sqft
    FROM metrics a
    LEFT JOIN stores d
        ON a.store_id = d.id
),
joined AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.brand,
        b.stock_qty
    FROM enriched e
    INNER JOIN products b
        ON e.product_id = b.id
    WHERE b.stock_qty < 157
)
SELECT
    id,
    quantity,
    last_restock,
    rn,
    prev_quantity,
    next_quantity,
    opened_date,
    sqft,
    brand
FROM joined
ORDER BY id DESC NULLS LAST
