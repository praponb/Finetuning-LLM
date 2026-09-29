WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM payments
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY amount NULLS FIRST) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        RANK() OVER (PARTITION BY status ORDER BY amount NULLS FIRST) AS amount_rank,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('week', paid_at) ORDER BY amount DESC) AS amount_percent_rank
    FROM latest
    WHERE method = 'bank_transfer'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY amount_percent_rank DESC NULLS FIRST, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount_rank DESC, id) AS event_seq,
        AVG(amount_percent_rank) OVER w_ordered AS amount_percent_rank_avg_2_prior,
        STDDEV_SAMP(amount_rank) OVER (PARTITION BY order_id) AS order_id_stddev_samp_amount_rank
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY order_id, method ORDER BY amount_percent_rank, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_seq,
        order_id_stddev_samp_amount_rank,
        CEIL(order_id_stddev_samp_amount_rank) AS order_id_stddev_samp_amount_rank_whole,
        ROUND(event_seq, 0) AS event_seq_rounded,
        COALESCE(method, 'OTHER') AS method_filled
    FROM with_windows
    WHERE amount_percent_rank <= 0.29
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_seq,
        order_id_stddev_samp_amount_rank,
        event_seq_rounded,
        method_filled,
        CASE WHEN order_id_stddev_samp_amount_rank >= 7 THEN 'High' WHEN order_id_stddev_samp_amount_rank >= 2 THEN 'Medium' ELSE 'Low' END AS order_id_stddev_samp_amount_rank_band
    FROM base
    WHERE event_seq_rounded >= 51
)
-- Multi-level aggregation
SELECT
    CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    CASE WHEN event_seq_rounded >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_rounded_tier,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    ROUND(AVG(event_seq), 2) AS avg_event_seq_2,
    COUNT(DISTINCT id) AS distinct_ids_2,
    AVG(order_id_stddev_samp_amount_rank) AS avg_order_id_stddev_samp_amount_rank
FROM valid
GROUP BY CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN event_seq_rounded >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END;
