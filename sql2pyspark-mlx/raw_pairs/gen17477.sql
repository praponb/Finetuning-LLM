WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        COALESCE(carrier, 'Unknown') AS carrier_filled
    FROM shipments
    WHERE delivered_date > TO_DATE('2023-07-28')
      AND order_id IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT id, delivered_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id ASC) AS row_num
        FROM valid
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY delivered_date ASC, id ASC) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY delivered_date DESC, id ASC) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY delivered_date ASC, id ASC) AS row_seq_2
    FROM ranked
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM scored
    INTERSECT ALL
    SELECT id
    FROM shipments
    WHERE carrier NOT IN ('UPS', 'FedEx', 'DHL')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        delivered_date,
        seq_no,
        DENSE_RANK() OVER (ORDER BY seq_no DESC NULLS LAST) AS seq_no_dense_rank
    FROM reconciled
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        delivered_date,
        ROUND(seq_no_dense_rank / SUM(seq_no_dense_rank) OVER (PARTITION BY DATE_TRUNC('WEEK', delivered_date)), 4) AS seq_no_dense_rank_share,
        PERCENT_RANK() OVER (ORDER BY seq_no_dense_rank DESC) AS seq_no_dense_rank_percent_rank,
        RANK() OVER (ORDER BY seq_no DESC NULLS LAST) AS seq_no_rank,
        RANK() OVER w_ordered AS seq_no_rank_2
    FROM with_windows
    WINDOW w_ordered AS (ORDER BY seq_no DESC NULLS FIRST)
),
metrics AS (
    -- Ranking and gap features
    SELECT *, RANK() OVER (PARTITION BY MONTH(delivered_date) ORDER BY seq_no_dense_rank_share DESC) AS seq_no_dense_rank_share_rank
    FROM enriched
    WHERE delivered_date BETWEEN '2023-10-26' AND '2023-10-30'
)
SELECT
    id,
    delivered_date,
    seq_no_dense_rank_share,
    seq_no_dense_rank_percent_rank,
    seq_no_rank,
    seq_no_rank_2
FROM metrics
