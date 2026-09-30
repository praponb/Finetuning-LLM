WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
),
annotated AS (
    -- Add calculated columns
    SELECT *, TO_DATE(reported_at) AS reported_at_date
    FROM filtered_claims
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
      AND s.adjuster_id <= 4
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.reported_at,
        s.claim_date_label,
        s.greatest_claimed_amount_approved_amount,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_date,
        c.adjuster_id AS claim_adjuster_id,
        c.approved_amount AS claim_approved_amount
    FROM qualified s
    JOIN claims c ON c.policy_id = s.policy_id AND c.adjuster_id >= 3
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(claim_date) AS claim_date_year,
        claim_id,
        adjuster_id,
        COUNT(*) AS n_rows,
        SUM(claim_approved_amount) AS total_claim_approved_amount,
        MIN(greatest_claimed_amount_approved_amount) AS min_greatest_claimed_amount_approved_amount,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE claim_status NOT IN ('OPEN', 'IN_REVIEW')) AS n_claim_status_not_in_open_in
    FROM with_children_claims
    WHERE adjuster_id <= 7
    GROUP BY YEAR(claim_date), claim_id, adjuster_id
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM per_group
    WHERE min_greatest_claimed_amount_approved_amount > (SELECT MIN(min_greatest_claimed_amount_approved_amount) FROM per_group)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        claim_date_year,
        claim_id,
        adjuster_id,
        n_rows,
        total_claim_approved_amount,
        min_greatest_claimed_amount_approved_amount,
        n_claim_status_not_in_open_in,
        SUM(n_rows) OVER (PARTITION BY n_claim_status_not_in_open_in, claim_date_year ORDER BY distinct_ids, claim_date_year, claim_id, adjuster_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_rows,
        RANK() OVER (PARTITION BY n_claim_status_not_in_open_in, claim_date_year ORDER BY distinct_ids) AS distinct_ids_rank,
        ROUND(CUME_DIST() OVER w_claim_date_year_by_n_claim_s, 3) AS n_claim_status_not_in_open_in_cume_dist,
        SUM(total_claim_approved_amount) OVER (PARTITION BY n_claim_status_not_in_open_in, claim_date_year ORDER BY distinct_ids, claim_date_year, claim_id, adjuster_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_claim_approved_amount,
        MAX(n_rows) OVER (PARTITION BY n_claim_status_not_in_open_in, claim_date_year ORDER BY distinct_ids, claim_date_year, claim_id, adjuster_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS n_rows_max_3,
        SUM(claim_date_year) OVER (PARTITION BY n_claim_status_not_in_open_in, claim_date_year ORDER BY distinct_ids, claim_date_year, claim_id, adjuster_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS claim_date_year_sum_4
    FROM filtered
    WINDOW w_claim_date_year_by_n_claim_s AS (PARTITION BY claim_date_year ORDER BY n_claim_status_not_in_open_in DESC)
)
SELECT
    claim_date_year,
    claim_id,
    adjuster_id,
    total_claim_approved_amount,
    min_greatest_claimed_amount_approved_amount,
    n_claim_status_not_in_open_in,
    running_n_rows,
    distinct_ids_rank,
    n_rows_max_3,
    claim_date_year_sum_4
FROM with_trends
WHERE claim_id <= 90
