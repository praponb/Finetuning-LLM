WITH recent AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE policy_id <= 19
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        approved_amount,
        status,
        COUNT(*) OVER (PARTITION BY claim_type, adjuster_id) AS rows_in_claim_type_adjuster_id,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_claim_type_adjuster_id, 0), 4) AS claimed_amount_share_of_claim_type_adjuster_id,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY claim_type, adjuster_id), 0), 4) AS approved_amount_share_of_claim_type_adjuster_id,
        SUM(claimed_amount) OVER (PARTITION BY status, adjuster_id ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        LEAD(reported_at, 2) OVER (PARTITION BY status ORDER BY claim_date, id) AS next2_reported_at,
        DENSE_RANK() OVER (PARTITION BY policy_id ORDER BY reported_at NULLS LAST) AS reported_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY MONTH(claim_date), status ORDER BY claim_date ASC, id))) / 60, 1) AS minutes_since_prev_reported_at,
        LEAD(approved_amount, 2) OVER (PARTITION BY policy_id ORDER BY reported_at NULLS LAST, id) AS next2_approved_amount,
        NTILE(3) OVER (PARTITION BY status, adjuster_id ORDER BY approved_amount DESC, id) AS approved_amount_tercile,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_reported_at_year, 2) AS claimed_amount_vs_reported_at_year_avg,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY YEAR(reported_at)), 0), 4) AS approved_amount_share_of_reported_at_year,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY YEAR(reported_at)), 2) AS approved_amount_vs_reported_at_year_avg
    FROM recent
    WINDOW w_claim_type_adjuster_id AS (PARTITION BY claim_type, adjuster_id), w_reported_at_year AS (PARTITION BY YEAR(reported_at))
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(approved_amount_tercile, 0) AS approved_amount_tercile_filled,
        ABS(reported_at_dense_rank - 5) AS reported_at_dense_rank_distance
    FROM windowed
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_approved_amount_tercile_fill AS rn,
        MAX(next2_approved_amount) OVER (PARTITION BY policy_id) AS policy_id_max_next2_approved_amount,
        NTILE(5) OVER (PARTITION BY approved_amount_tercile_filled, reported_at_dense_rank_distance ORDER BY reported_at_dense_rank ASC, id) AS reported_at_dense_rank_quintile,
        NTILE(5) OVER (PARTITION BY status ORDER BY approved_amount_share_of_reported_at_year DESC, id) AS approved_amount_share_of_reported_at_year_quinti
    FROM scored
    WINDOW w_approved_amount_tercile_fill AS (PARTITION BY approved_amount_tercile_filled, reported_at_dense_rank_distance ORDER BY reported_at_dense_rank ASC, id)
),
summary AS (
    -- Summarise per group
    SELECT
        status,
        policy_id,
        ROUND(AVG(approved_amount_share_of_claim_type_adjuster_id), 2) AS avg_approved_amount_share_of_claim_type_adjuster,
        ROUND(SUM(approved_amount_tercile) / NULLIF(SUM(claimed_amount_share_of_claim_type_adjuster_id), 0), 3) AS approved_amount_tercile_to_claimed_amount_share_
    FROM sequenced
    WHERE reported_at_dense_rank_distance > 4
    GROUP BY status, policy_id
)
SELECT status, policy_id, approved_amount_tercile_to_claimed_amount_share_
FROM summary
ORDER BY policy_id DESC, approved_amount_tercile_to_claimed_amount_share_
