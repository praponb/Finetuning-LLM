WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY started_at DESC) AS rn
        FROM sessions
    ) ranked
    WHERE rn = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT browser, id
    FROM latest
    EXCEPT ALL
    SELECT browser, id
    FROM sessions
    WHERE browser <> 'Chrome'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.browser = k.browser AND s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        started_at,
        rn,
        CAST(rn AS STRING) AS rn_str
    FROM kept_rows
    WHERE country IS NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.segment,
        c.credit_limit,
        c.name
    FROM valid src
    LEFT JOIN customers c
        ON src.customer_id = c.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        segment,
        credit_limit
    FROM with_dims
) src
PIVOT (
    SUM(credit_limit) AS total,
    COUNT(1) AS item_count
    FOR segment IN ('Consumer' AS consumer, 'Corporate' AS corporate, 'SMB' AS smb)
);
