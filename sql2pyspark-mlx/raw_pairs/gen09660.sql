WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(quantity, TRUE) OVER w_grp AS first_known_quantity,
        LAG(quantity, 1) OVER (PARTITION BY DATE_TRUNC('month', last_restock), store_id ORDER BY quantity ASC, id ASC) AS prev_quantity,
        ROW_NUMBER() OVER (PARTITION BY YEAR(last_restock), product_id ORDER BY last_restock DESC, id ASC) AS event_seq
    FROM inventory
    WHERE quantity >= 68
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.*,
        d.store_name,
        d.region,
        d.sqft
    FROM metrics src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock ASC, id ASC) AS latest_rank
        FROM enriched
    ) t
    WHERE latest_rank = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        first_known_quantity,
        prev_quantity,
        event_seq,
        store_name,
        region,
        latest_rank,
        CEIL(event_seq) AS event_seq_whole,
        ROUND(sqft * 0.9, 2) AS sqft_adjusted,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM deduped
    WHERE region IS NOT NULL
      AND store_id IN (1, 2)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        prev_quantity,
        event_seq,
        store_name,
        region,
        latest_rank,
        event_seq_whole,
        sqft_adjusted
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity ASC NULLS FIRST, id ASC) = 1
)
SELECT
    id,
    prev_quantity,
    store_name,
    latest_rank,
    event_seq_whole,
    sqft_adjusted
FROM ranked
ORDER BY event_seq_whole ASC NULLS LAST;
