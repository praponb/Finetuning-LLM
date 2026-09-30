WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status,
        p.segment AS holder_segment
    FROM prepared s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(start_date) OVER (PARTITION BY holder_segment, status ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        LAG(start_date) OVER w_holder_segment_by_start_date AS prev_start_date,
        ROW_NUMBER() OVER (PARTITION BY holder_segment, status ORDER BY start_date DESC, id) AS rn,
        ROUND(PERCENT_RANK() OVER w_holder_segment_by_start_date, 3) AS start_date_percent_rank,
        DATEDIFF(start_date, LAG(start_date) OVER (ORDER BY start_date NULLS LAST, id)) AS days_since_prev_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY holder_segment ORDER BY start_date DESC NULLS FIRST, id)) AS days_since_prev_start_date_3,
        LEAD(start_date, 2) OVER w_holder_segment_status_by_sta_2 AS next2_start_date,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY holder_segment ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date
    FROM with_parent_holder
    WINDOW w_holder_segment_by_start_date AS (PARTITION BY holder_segment ORDER BY start_date DESC NULLS FIRST, id), w_holder_segment_status_by_sta_2 AS (PARTITION BY holder_segment, status ORDER BY start_date DESC, id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        start_date,
        status,
        holder_segment,
        prev_start_date,
        rn,
        days_since_prev_start_date,
        days_since_prev_start_date_3,
        next2_start_date,
        ROUND(rn / NULLIF(SUM(rn) OVER (PARTITION BY MONTH(next2_start_date), rn), 0), 4) AS rn_share_of_next2_start_date_month_rn,
        ROUND(days_since_prev_start_date / NULLIF(SUM(days_since_prev_start_date) OVER (PARTITION BY MONTH(next2_start_date), rn), 0), 4) AS days_since_prev_start_date_share_of_next2_start_,
        DATEDIFF(first_start_date, LAG(first_start_date) OVER w_rn_by_start_date) AS days_since_prev_first_start_date,
        SIZE(COLLECT_SET(holder_segment) OVER (PARTITION BY MONTH(next2_start_date), rn)) AS distinct_holder_segment_in_next2_start_date_mont,
        SUM(days_since_prev_start_date) OVER (PARTITION BY rn ORDER BY start_date ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS days_since_prev_start_date_sum_3_prior,
        RANK() OVER (ORDER BY first_start_date DESC NULLS FIRST) AS first_start_date_rank,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY MONTH(next2_start_date), rn)) AS distinct_status_in_next2_start_date_month_rn
    FROM windowed
    WINDOW w_rn_by_start_date AS (PARTITION BY rn ORDER BY start_date ASC NULLS LAST, id)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        first_start_date_rank,
        distinct_holder_segment_in_next2_start_date_mont,
        ARRAY_SORT(COLLECT_SET(holder_segment)) AS holder_segment_list,
        COUNT(*) AS n_rows,
        ROUND(AVG(distinct_holder_segment_in_next2_start_date_mont), 2) AS avg_distinct_holder_segment_in_next2_start_date_,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        COUNT(DISTINCT holder_segment) AS distinct_holder_segments
    FROM sequenced
    GROUP BY 1, 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(distinct_holder_segments) OVER (PARTITION BY distinct_holder_segments ORDER BY avg_distinct_holder_segment_in_next2_start_date_, first_start_date_rank, distinct_holder_segment_in_next2_start_date_mont ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_holder_segments,
        MIN(distinct_holder_ids) OVER w_n_rows_distinct_holder_segme AS n_rows_distinct_holder_segments_min_distinct_hol
    FROM rollup_base
    WINDOW w_n_rows_distinct_holder_segme AS (PARTITION BY n_rows, distinct_holder_segments)
)
-- Final projection
SELECT
    first_start_date_rank,
    distinct_holder_segment_in_next2_start_date_mont,
    n_rows,
    avg_distinct_holder_segment_in_next2_start_date_,
    distinct_holder_ids,
    distinct_holder_segments,
    first_distinct_holder_segments,
    FLOOR(first_start_date_rank / 2) * 2 AS first_start_date_rank_bucket
FROM ranked
