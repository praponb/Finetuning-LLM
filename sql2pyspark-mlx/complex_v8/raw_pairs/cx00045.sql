WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        DATE_TRUNC('YEAR', claim_date) AS claim_date_year
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 8 AND 10
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(approved_amount, 2) OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        COUNT(*) OVER w_claim_date_year_to_date AS rows_in_claim_date_year_to_date,
        approved_amount - LAG(approved_amount) OVER (ORDER BY claimed_amount, id) AS approved_amount_change
    FROM base
    WINDOW w_claim_date_year_to_date AS (PARTITION BY TO_DATE(claim_date_year))
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM windowed s
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        rows_in_claim_date_year_to_date,
        approved_amount_change,
        claim_count,
        SUM(second_approved_amount) OVER (PARTITION BY policy_id ORDER BY rows_in_claim_date_year_to_date ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS second_approved_amount_sum_2_prior,
        MAX(claimed_amount) OVER (PARTITION BY claim_count, DATE_TRUNC('MONTH', claim_date_year) ORDER BY claimed_amount DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_1_prior,
        SIZE(COLLECT_SET(claim_type) OVER w_adjuster_id_policy_id) AS distinct_claim_type_in_adjuster_id_policy_id,
        SIZE(COLLECT_SET(status) OVER w_adjuster_id_policy_id) AS distinct_status_in_adjuster_id_policy_id,
        NTILE(4) OVER (PARTITION BY claim_count, DATE_TRUNC('MONTH', claim_date_year) ORDER BY claimed_amount DESC, id) AS claimed_amount_quartile
    FROM profiled
    WINDOW w_adjuster_id_policy_id AS (PARTITION BY adjuster_id, policy_id)
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT SUM(c.approved_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS total_claim_approved_amount,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_date,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_2
    FROM sequenced s
),
with_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.approved_amount_change,
        s.second_approved_amount_sum_2_prior,
        s.claimed_amount_max_1_prior,
        s.distinct_claim_type_in_adjuster_id_policy_id,
        s.distinct_status_in_adjuster_id_policy_id,
        s.claimed_amount_quartile,
        s.last_claim_date,
        s.claim_count_2,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM summarized_rows s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        status,
        second_approved_amount_sum_2_prior,
        claimed_amount_max_1_prior,
        distinct_claim_type_in_adjuster_id_policy_id,
        claimed_amount_quartile,
        last_claim_date,
        LOWER(TRIM(adjuster_name)) AS adjuster_name_clean,
        DATE_FORMAT(last_claim_date, 'MMM yyyy') AS last_claim_date_label,
        LENGTH(TRIM(adjuster_team)) AS adjuster_team_len
    FROM with_details_adjuster
)
-- Final projection
SELECT
    id,
    status,
    second_approved_amount_sum_2_prior,
    claimed_amount_max_1_prior,
    distinct_claim_type_in_adjuster_id_policy_id,
    claimed_amount_quartile,
    last_claim_date,
    adjuster_name_clean,
    last_claim_date_label,
    adjuster_team_len,
    SUBSTRING(TRIM(last_claim_date_label), 1, 4) AS last_claim_date_label_prefix
FROM tagged
