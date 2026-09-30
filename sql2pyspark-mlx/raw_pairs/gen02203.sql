WITH base AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(city, 1, 2) AS city_prefix
    FROM suppliers
    WHERE city IS NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        supplier_name,
        rating
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating ASC, id ASC) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        ROW_NUMBER() OVER (ORDER BY rating ASC, id ASC) AS seq_no,
        AVG(rating) OVER (ORDER BY rating ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rating_avg_2,
        AVG(rating) OVER (ORDER BY rating ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rating_avg_3_prior,
        SUM(rating) OVER (ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM deduped
    WHERE rating BETWEEN 2.1 AND 3.4
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY rating_avg_3_prior ASC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY cumulative_rating ASC, id ASC) AS row_seq
    FROM enriched
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.stock_qty < 304
        )
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        seq_no,
        AVG(event_seq) AS avg_event_seq,
        ROUND(AVG(cumulative_rating), 2) AS avg_cumulative_rating,
        COUNT(*) AS event_count,
        COUNT(*) AS row_count,
        APPROX_PERCENTILE(cumulative_rating, ARRAY(0.25, 0.5, 0.75)) AS cumulative_rating_quartiles
    FROM not_flagged
    GROUP BY seq_no
)
SELECT
    seq_no,
    avg_event_seq,
    avg_cumulative_rating,
    event_count,
    row_count,
    cumulative_rating_quartiles
FROM grouped
ORDER BY event_count DESC;
