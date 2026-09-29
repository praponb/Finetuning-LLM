WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(supplier_name, 'Unknown') AS supplier_name_filled,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM suppliers
    WHERE city IS DISTINCT FROM 'Bangkok'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.launch_date >= '2024-12-01'
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        city,
        supplier_name_filled,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating ASC, id ASC) AS seq_no,
        STDDEV_SAMP(rating) OVER (PARTITION BY city) AS city_stddev_samp_rating
    FROM not_flagged
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost BETWEEN 250 AND 360
        )
),
not_flagged_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand = 'Stark'
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(city_stddev_samp_rating) OVER (PARTITION BY city ORDER BY seq_no ASC, id ASC) AS next_city_stddev_samp_rating,
        LAG(city_stddev_samp_rating) OVER (PARTITION BY city ORDER BY seq_no ASC, id ASC) AS prev_city_stddev_samp_rating,
        LAG(seq_no) OVER (PARTITION BY city ORDER BY seq_no ASC, id ASC) AS prev_seq_no,
        LEAD(seq_no) OVER (PARTITION BY city ORDER BY seq_no ASC, id ASC) AS next_seq_no,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY seq_no ASC, id ASC) AS event_seq
    FROM not_flagged_suppliers
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(event_seq) OVER (ORDER BY next_city_stddev_samp_rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_seq,
        NTILE(5) OVER (PARTITION BY city ORDER BY seq_no DESC, id ASC) AS seq_no_bucket,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY seq_no DESC NULLS FIRST) AS seq_no_dense_rank
    FROM metrics
)
-- Ranking and gap features
SELECT
    id,
    supplier_name_filled,
    city_stddev_samp_rating,
    next_city_stddev_samp_rating,
    prev_city_stddev_samp_rating,
    prev_seq_no,
    next_seq_no,
    seq_no_bucket,
    NTILE(4) OVER (PARTITION BY city ORDER BY next_city_stddev_samp_rating DESC, id ASC) AS next_city_stddev_samp_rating_bucket,
    ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY city), 4) AS event_seq_share,
    RANK() OVER (PARTITION BY city ORDER BY prev_seq_no DESC NULLS FIRST) AS prev_seq_no_rank,
    SUM(cumulative_event_seq) OVER (PARTITION BY city ORDER BY seq_no_dense_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_event_seq
FROM enriched;
