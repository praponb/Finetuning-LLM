WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        LEAD(revenue) OVER w_running next_revenue,
        LAG(units) OVER w_running prev_units,
        LEAD(units) OVER w_running next_units,
        ROW_NUMBER() OVER w_running row_seq
    FROM store_sales
    WINDOW w_running AS (PARTITION BY product_id ORDER BY units, id)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_id,
        sale_date,
        units,
        next_revenue,
        prev_units
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS latest_rank
        FROM with_windows
    ) x
    WHERE latest_rank = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN units >= 39 THEN 'High' WHEN units >= 13 THEN 'Medium' ELSE 'Low' END units_band,
        COALESCE(units, 0) units_filled
    FROM deduped
    WHERE sale_date >= DATE '2023-03-09'
      AND units > 38
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.sqft,
        d.opened_date
    FROM valid s
    LEFT JOIN stores d
        ON s.store_id = d.id
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.*,
        d.store_name,
        d.opened_date AS store_opened_date,
        d.city
    FROM with_dims e
    LEFT JOIN stores d
        ON e.store_id = d.id
    WHERE YEAR(d.opened_date) = 2023
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            store_id,
            city,
            next_revenue
        FROM joined
    ) src
    PIVOT (
        SUM(next_revenue) AS amount
        FOR city IN ('London', 'Berlin', 'Bangkok')
    )
)
SELECT
    s.store_id,
    s.London,
    s.Berlin,
    s.Bangkok,
    (SELECT MIN(o2.shipping_fee) FROM orders o2 WHERE o2.store_id = s.store_id) AS min_order_shipping_fee
FROM pivoted s
ORDER BY London NULLS FIRST
