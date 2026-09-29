WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE product IN ('HOME', 'AUTO', 'HEALTH')
      AND status IN ('CANCELLED', 'EXPIRED', 'ACTIVE')
),
joined AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status,
        p.region AS holder_region
    FROM valid_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(start_date) OVER (ORDER BY start_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY holder_id, holder_region ORDER BY start_date, id)) AS days_since_prev_start_date,
        ROW_NUMBER() OVER w_holder_region_by_start_date AS rn,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY holder_region ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        LAST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY start_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        LAG(start_date, 2) OVER (PARTITION BY holder_id, holder_region ORDER BY start_date, id) AS prev2_start_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, holder_region ORDER BY start_date, id), 3) AS start_date_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY holder_region ORDER BY start_date DESC, id) AS seq_no,
        LEAD(start_date, 2) OVER w_holder_id_holder_region_by_s AS next2_start_date
    FROM joined
    WINDOW w_holder_region_by_start_date AS (PARTITION BY holder_region ORDER BY start_date, id), w_holder_id_holder_region_by_s AS (PARTITION BY holder_id, holder_region ORDER BY start_date, id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        start_date,
        status,
        first_start_date,
        days_since_prev_start_date,
        rn,
        second_start_date,
        last_known_start_date,
        prev2_start_date,
        start_date_percent_rank,
        seq_no,
        next2_start_date,
        ROUND(days_since_prev_start_date - AVG(days_since_prev_start_date) OVER (PARTITION BY holder_region, seq_no), 2) AS days_since_prev_start_date_vs_holder_region_seq_,
        SIZE(COLLECT_SET(holder_id) OVER w_holder_region_seq_no) AS distinct_holder_id_in_holder_region_seq_no,
        MAX(seq_no) OVER (PARTITION BY holder_region, seq_no) AS holder_region_seq_no_max_seq_no,
        ROUND(CUME_DIST() OVER w_holder_id_by_prev2_start_dat, 3) AS prev2_start_date_cume_dist
    FROM with_trends
    WINDOW w_holder_region_seq_no AS (PARTITION BY holder_region, seq_no), w_holder_id_by_prev2_start_dat AS (PARTITION BY holder_id ORDER BY prev2_start_date NULLS LAST)
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(prev2_start_date, 'yyyy-MM') AS prev2_start_date_ym,
        DATE_FORMAT(first_start_date, 'yyyy-MM') AS first_start_date_ym,
        ROUND(STDDEV(rn), 2) AS stddev_rn,
        ROUND(SUM(rn) / NULLIF(SUM(days_since_prev_start_date), 0), 3) AS rn_to_days_since_prev_start_date_ratio,
        COUNT(*) FILTER (WHERE status = 'ACTIVE') AS n_status_active,
        COUNT(DISTINCT status) AS distinct_status
    FROM with_windows
    WHERE start_date_percent_rank >= 0.3
    GROUP BY DATE_FORMAT(prev2_start_date, 'yyyy-MM'), DATE_FORMAT(first_start_date, 'yyyy-MM')
)
SELECT
    prev2_start_date_ym,
    first_start_date_ym,
    stddev_rn,
    rn_to_days_since_prev_start_date_ratio,
    n_status_active,
    LOWER(REGEXP_REPLACE(TRIM(prev2_start_date_ym), ' ', '_')) AS prev2_start_date_ym_slug
FROM grouped
ORDER BY stddev_rn NULLS LAST
