WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE (end_date >= DATE '2026-01-20' OR status NOT IN ('LAPSED', 'CANCELLED'))
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        c.id AS claim_id,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id
    FROM recent_policys s
    JOIN claims c ON c.policy_id = s.id AND c.status = 'APPROVED'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_end_date_year_by_end_date_ti, 3) AS end_date_percent_rank,
        FIRST_VALUE(end_date) OVER (PARTITION BY YEAR(end_date) ORDER BY end_date DESC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        DENSE_RANK() OVER (PARTITION BY claim_id, holder_id ORDER BY end_date DESC) AS end_date_dense_rank,
        RANK() OVER (PARTITION BY claim_id, holder_id ORDER BY end_date DESC) AS end_date_rank,
        ROW_NUMBER() OVER (PARTITION BY claim_id, holder_id ORDER BY end_date DESC, id, claim_id) AS rn,
        LAG(end_date, 2) OVER (PARTITION BY claim_id, holder_id ORDER BY end_date DESC, id, claim_id) AS prev2_end_date
    FROM detail_rows
    WINDOW w_end_date_year_by_end_date_ti AS (PARTITION BY YEAR(end_date) ORDER BY end_date DESC)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        end_date_dense_rank,
        MAX(end_date_percent_rank) AS max_end_date_percent_rank,
        COUNT(*) FILTER (WHERE claim_status <> 'APPROVED') AS n_claim_status_approved,
        COUNT(*) AS cnt,
        DATEDIFF(MAX(prev2_end_date), MIN(prev2_end_date)) AS prev2_end_date_span_days
    FROM ranked
    GROUP BY end_date_dense_rank
    HAVING COUNT(*) >= 2
),
annotated AS (
    -- Add calculated columns
    SELECT
        end_date_dense_rank,
        max_end_date_percent_rank,
        cnt,
        prev2_end_date_span_days,
        end_date_dense_rank + n_claim_status_approved AS end_date_dense_rank_plus_n_claim_status_approved,
        GREATEST(n_claim_status_approved, cnt) AS greatest_n_claim_status_approved_cnt
    FROM rollup_base
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM annotated
    WHERE max_end_date_percent_rank > (SELECT AVG(max_end_date_percent_rank) FROM annotated)
),
enriched AS (
    -- Derive helper columns
    SELECT *, end_date_dense_rank - cnt AS end_date_dense_rank_minus_cnt
    FROM screened
)
-- Final projection
SELECT
    end_date_dense_rank,
    max_end_date_percent_rank,
    cnt,
    prev2_end_date_span_days,
    end_date_dense_rank_plus_n_claim_status_approved,
    greatest_n_claim_status_approved_cnt
FROM enriched
ORDER BY end_date_dense_rank DESC, greatest_n_claim_status_approved_cnt
