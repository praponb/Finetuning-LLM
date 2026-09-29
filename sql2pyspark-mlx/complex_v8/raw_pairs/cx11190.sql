WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        UPPER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE claimed_amount > 5313.8
      AND claim_date BETWEEN DATE '2024-02-17' AND DATE '2025-05-30'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek,
        LENGTH(TRIM(claim_type_clean)) AS claim_type_clean_len
    FROM cleaned_claims
),
latest_per_key AS (
    -- Keep the latest row per policy_id, claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        claim_type_clean,
        claim_date_dayofweek,
        claim_type_clean_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, claim_type ORDER BY claim_date DESC, id DESC) AS recency_rank
        FROM labeled
    ) ranked
    WHERE recency_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)) AS claim_date_month_max_claimed_amount,
        COUNT(*) OVER w_claim_date_month AS rows_in_claim_date_month,
        NTILE(3) OVER w_policy_id_claim_type_clean_l AS reported_at_tercile,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)), 2) AS claimed_amount_vs_claim_date_month_avg
    FROM latest_per_key
    WINDOW w_claim_date_month AS (PARTITION BY DATE_TRUNC('MONTH', claim_date)), w_policy_id_claim_type_clean_l AS (PARTITION BY policy_id, claim_type_clean_len ORDER BY reported_at DESC, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        claim_type,
        claim_date_dayofweek,
        policy_id,
        SUM(claim_date_month_max_claimed_amount) AS total_claim_date_month_max_claimed_amount,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        ROUND(SUM(claim_date_month_max_claimed_amount) / NULLIF(SUM(reported_at_tercile), 0), 3) AS claim_date_month_max_claimed_amount_to_reported_,
        ROUND(AVG(rows_in_claim_date_month), 2) AS avg_rows_in_claim_date_month
    FROM metrics
    GROUP BY claim_type, claim_date_dayofweek, policy_id
)
SELECT *
FROM (
    SELECT policy_id, claim_type, claim_date_dayofweek
    FROM rollup_base
) src
PIVOT (
    MAX(claim_date_dayofweek) AS max_v, COUNT(claim_date_dayofweek) AS n
    FOR claim_type IN ('theft', 'fire')
)
ORDER BY policy_id
