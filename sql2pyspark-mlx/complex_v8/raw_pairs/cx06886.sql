WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        QUARTER(start_date) AS start_date_quarter,
        ROUND(premium * 1.5, 2) AS premium_adj
    FROM policies
    WHERE status NOT IN ('CANCELLED', 'EXPIRED')
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.start_date_quarter,
        s.premium_adj,
        p.segment AS holder_segment
    FROM valid s
    JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(premium_adj, 2) OVER (PARTITION BY holder_id ORDER BY premium_adj DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_adj,
        MIN(start_date_quarter) OVER (PARTITION BY YEAR(start_date), holder_id) AS start_date_year_holder_id_min_start_date_quarter,
        ROUND(PERCENT_RANK() OVER w_start_date_quarter_by_start, 3) AS start_date_percent_rank,
        NTILE(5) OVER (PARTITION BY holder_id ORDER BY premium_adj DESC NULLS FIRST, id) AS premium_adj_quintile,
        ROUND(AVG(start_date_quarter) OVER (PARTITION BY start_date_quarter, holder_id), 2) AS start_date_quarter_holder_id_avg_start_date_quar,
        ROUND(start_date_quarter / NULLIF(SUM(start_date_quarter) OVER (PARTITION BY start_date_quarter, holder_id), 0), 4) AS start_date_quarter_share_of_start_date_quarter_h,
        LAG(start_date) OVER (PARTITION BY holder_segment, status ORDER BY end_date ASC, id) AS prev_start_date,
        ROUND(premium_adj / NULLIF(SUM(premium_adj) OVER w_start_date_year_holder_id, 0), 4) AS premium_adj_share_of_start_date_year_holder_id,
        LEAD(start_date_quarter, 2) OVER w_start_date_quarter_by_start AS next2_start_date_quarter,
        COUNT(*) OVER w_start_date_year_holder_id AS rows_in_start_date_year_holder_id
    FROM with_parent_holder
    WINDOW w_start_date_quarter_by_start AS (PARTITION BY start_date_quarter ORDER BY start_date, id), w_start_date_year_holder_id AS (PARTITION BY YEAR(start_date), holder_id)
)
-- Final projection
SELECT
    id,
    start_date,
    end_date,
    start_date_quarter,
    holder_segment,
    second_premium_adj,
    start_date_percent_rank,
    premium_adj_quintile,
    start_date_quarter_holder_id_avg_start_date_quar,
    start_date_quarter_share_of_start_date_quarter_h,
    prev_start_date,
    next2_start_date_quarter,
    rows_in_start_date_year_holder_id,
    premium_adj + next2_start_date_quarter AS premium_adj_plus_next2_start_date_quarter,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown
FROM sequenced
WHERE premium_adj < 5675.0
