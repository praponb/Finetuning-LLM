WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount
    FROM claims
    WHERE claim_date < DATE '2025-04-11'
      AND approved_amount <= 14595.6
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        claim_date_label,
        claimed_amount_minus_approved_amount,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY status ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status)) AS distinct_adjuster_id_in_status,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status), 2) AS approved_amount_vs_status_avg,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY adjuster_id, status ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        LEAD(claim_date, 2) OVER w_claim_type_policy_id_by_appr AS next2_claim_date
    FROM valid_claims
    WINDOW w_claim_type_policy_id_by_appr AS (PARTITION BY claim_type, policy_id ORDER BY approved_amount, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        claim_date_label,
        claimed_amount_minus_approved_amount,
        second_claim_date,
        distinct_adjuster_id_in_status,
        approved_amount_vs_status_avg,
        first_known_claimed_amount,
        rows_in_status,
        MAX(first_known_claimed_amount) OVER (PARTITION BY YEAR(next2_claim_date), status) AS next2_claim_date_year_status_max_first_known_cla,
        LEAD(claimed_amount) OVER w_next2_claim_date_month_statu AS next_claimed_amount,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY YEAR(next2_claim_date), status)) AS distinct_claim_type_in_next2_claim_date_year_sta,
        COUNT(*) OVER w_next2_claim_date_year_status AS rows_in_next2_claim_date_year_status,
        SUM(first_known_claimed_amount) OVER (PARTITION BY MONTH(next2_claim_date), status ORDER BY claimed_amount ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS first_known_claimed_amount_sum_2
    FROM sequenced
    WINDOW w_next2_claim_date_month_statu AS (PARTITION BY MONTH(next2_claim_date), status ORDER BY claimed_amount ASC, id), w_next2_claim_date_year_status AS (PARTITION BY YEAR(next2_claim_date), status)
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(first_known_claimed_amount, 2) OVER (PARTITION BY distinct_adjuster_id_in_status, YEAR(second_claim_date) ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_known_claimed_amount,
        NTILE(10) OVER (PARTITION BY distinct_adjuster_id_in_status, YEAR(second_claim_date) ORDER BY claimed_amount DESC, id) AS claimed_amount_decile,
        NTILE(4) OVER w_distinct_adjuster_id_in_stat_2 AS claimed_amount_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_adjuster_id_in_status, YEAR(second_claim_date) ORDER BY claimed_amount DESC, id), 3) AS claimed_amount_percent_rank
    FROM metrics
    WINDOW w_distinct_adjuster_id_in_stat_2 AS (PARTITION BY distinct_adjuster_id_in_status, YEAR(second_claim_date) ORDER BY claimed_amount DESC, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claimed_amount_quartile, claim_type, status, rows_in_status
    FROM sequenced_2
) src
PIVOT (
    AVG(rows_in_status) AS avg_v, SUM(rows_in_status) AS sum_v
    FOR status IN ('APPROVED', 'OPEN', 'IN_REVIEW', 'REJECTED')
)
ORDER BY claimed_amount_quartile, claim_type
