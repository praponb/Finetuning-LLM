WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        COALESCE(region, 'Unknown') AS region_filled,
        COALESCE(region, 'N/A') AS region_filled_2
    FROM stores
    WHERE city = 'Tokyo'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        sqft,
        SUM(sqft) OVER w_part AS running_sum_sqft,
        AVG(sqft) OVER w_part AS running_avg_sqft,
        COUNT(sqft) OVER w_part AS running_count_sqft,
        ROW_NUMBER() OVER (PARTITION BY MONTH(opened_date) ORDER BY sqft ASC, id ASC) AS row_seq
    FROM base
    WINDOW w_part AS (ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_stores AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY running_sum_sqft ASC) AS running_sum_sqft_dense_rank,
        LAST_VALUE(sqft, TRUE) OVER w_recent AS last_known_sqft
    FROM scored
    WINDOW w_recent AS (ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.revenue
    FROM scored_stores s
    JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.units IS NULL
        )
),
scored_stores_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        sqft,
        running_avg_sqft,
        row_seq,
        revenue,
        LEAD(sqft) OVER w_ordered AS next_sqft,
        LEAD(running_sum_sqft_dense_rank) OVER w_ordered AS next_running_sum_sqft_dense_rank,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM kept
    WHERE sqft BETWEEN 9077 AND 10106
    WINDOW w_ordered AS (ORDER BY sqft ASC, id ASC)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        sqft,
        running_avg_sqft,
        row_seq,
        next_running_sum_sqft_dense_rank,
        seq_no,
        DENSE_RANK() OVER (ORDER BY sqft DESC) AS sqft_dense_rank,
        AVG(next_sqft) OVER (ORDER BY sqft ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS next_sqft_avg_6
    FROM scored_stores_2
)
SELECT
    id,
    sqft,
    running_avg_sqft,
    row_seq,
    next_running_sum_sqft_dense_rank,
    seq_no,
    sqft_dense_rank,
    next_sqft_avg_6
FROM with_windows
ORDER BY seq_no DESC;
