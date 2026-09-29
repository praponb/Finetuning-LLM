WITH cleaned AS (
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
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE adjuster_id <= 2
      AND approved_amount >= 4554.8
),
stats AS (
    -- Group-level totals
    SELECT
        claim_type,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT claim_type) AS distinct_claim_types,
        MAX(claim_date) AS last_claim_date,
        COUNT(*) FILTER (WHERE claim_type = 'fire') AS n_claim_type_fire,
        COUNT(*) FILTER (WHERE claim_type IN ('flood', 'theft', 'delay')) AS n_claim_type_in_flood_theft_de
    FROM cleaned
    GROUP BY claim_type, DATE_TRUNC('MONTH', claim_date)
    HAVING SUM(greatest_approved_amount_claimed_amount) > 17500.0
),
annotated AS (
    -- Add calculated columns
    SELECT
        claim_type,
        claim_date_month,
        distinct_ids,
        distinct_claim_types,
        last_claim_date,
        n_claim_type_in_flood_theft_de,
        CASE WHEN claim_type IN ('collision', 'flood') THEN 'group_a' WHEN claim_type = 'fire' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        CASE WHEN claim_type IN ('collision', 'medical') THEN 'group_a' WHEN claim_type = 'theft' THEN 'group_b' ELSE 'other' END AS claim_type_group_2
    FROM stats
    WHERE distinct_claim_types >= 2
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM annotated
    WHERE n_claim_type_in_flood_theft_de > (SELECT MIN(n_claim_type_in_flood_theft_de) FROM annotated)
),
unique_rows AS (
    -- Keep the earliest row per claim_type_group, claim_type_group_2
    SELECT
        claim_type,
        claim_date_month,
        distinct_ids,
        distinct_claim_types,
        last_claim_date,
        n_claim_type_in_flood_theft_de,
        claim_type_group,
        claim_type_group_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type_group, claim_type_group_2 ORDER BY claim_date_month, claim_type, claim_date_month) AS recency_rank
        FROM with_activity
    ) ranked
    WHERE recency_rank = 1
),
tagged AS (
    -- Add calculated columns
    SELECT
        claim_type,
        claim_date_month,
        distinct_claim_types,
        n_claim_type_in_flood_theft_de,
        claim_type_group,
        claim_type_group_2,
        LENGTH(TRIM(claim_type_group_2)) AS claim_type_group_2_len
    FROM unique_rows
)
-- Final projection
SELECT
    claim_type,
    claim_date_month,
    n_claim_type_in_flood_theft_de,
    claim_type_group,
    claim_type_group_2,
    claim_type_group_2_len,
    ROUND(claim_type_group_2_len / NULLIF(distinct_claim_types, 0), 3) AS claim_type_group_2_len_per_distinct_claim_types,
    ABS(distinct_claim_types - 7) AS distinct_claim_types_distance
FROM tagged
WHERE claim_type_group_2_len > 6
ORDER BY claim_type NULLS LAST, claim_date_month
LIMIT 5
