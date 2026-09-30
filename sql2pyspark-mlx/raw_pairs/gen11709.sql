WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(cost) OVER w_running carrier_stddev_samp_cost,
        NTILE(4) OVER w_grp cost_bucket,
        NTILE(4) OVER (ORDER BY cost DESC, id ASC) cost_bucket_2
    FROM shipments
    WINDOW w_running AS (PARTITION BY carrier), w_grp AS (PARTITION BY status, carrier ORDER BY cost ASC, id ASC)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.cost,
        src.cost_bucket_2,
        p.order_date,
        p.amount,
        p.status AS order_status
    FROM with_windows src
    LEFT JOIN orders p
        ON src.order_id <=> p.id
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.cost,
        src.cost_bucket_2,
        src.order_date,
        src.amount,
        src.order_status,
        o.shipping_fee,
        o.status
    FROM enriched src
    LEFT JOIN orders o
        ON src.order_id = o.id
    WHERE o.channel <> 'web'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_date,
        amount,
        order_status,
        status,
        LAG(amount) OVER (PARTITION BY order_status, status ORDER BY order_date ASC, id ASC) prev_amount,
        LAG(cost) OVER (PARTITION BY order_status, status ORDER BY order_date ASC, id ASC) prev_cost,
        LEAD(cost) OVER (PARTITION BY order_status, status ORDER BY order_date ASC, id ASC) next_cost,
        ROW_NUMBER() OVER (PARTITION BY order_status, status ORDER BY order_date ASC, id ASC) seq_no
    FROM enriched_shipments
),
with_windows_shipments AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        status,
        prev_cost,
        seq_no,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY DATE_TRUNC('WEEK', order_date)), 4) seq_no_share,
        SUM(prev_amount) OVER (ORDER BY prev_amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_prev_amount,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY DATE_TRUNC('YEAR', order_date), order_status), 4) seq_no_share_2,
        RANK() OVER (ORDER BY seq_no DESC NULLS LAST) seq_no_rank
    FROM scored
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        status,
        prev_cost,
        seq_no_share,
        seq_no_share_2,
        AVG(seq_no_rank) OVER w_recent running_avg_seq_no_rank,
        MAX(seq_no_rank) OVER w_recent running_max_seq_no_rank,
        COUNT(seq_no_rank) OVER w_recent running_count_seq_no_rank,
        SUM(seq_no_rank) OVER w_recent running_sum_seq_no_rank
    FROM with_windows_shipments
    WINDOW w_recent AS (PARTITION BY status ORDER BY seq_no_share ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    status,
    CASE WHEN seq_no_share >= 0.83 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_share_tier,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) AS num_rows,
    MAX(running_count_seq_no_rank) AS max_running_count_seq_no_rank,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY MAX(running_count_seq_no_rank) DESC) AS max_running_count_seq_no_rank_percent_rank
FROM metrics
GROUP BY status, seq_no_share_tier
ORDER BY seq_no_share_tier ASC NULLS FIRST
