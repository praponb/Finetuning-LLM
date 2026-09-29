SELECT
    policy_id AS key_id,
    claim_date,
    claimed_amount,
    'claim' AS origin
FROM claims
UNION
SELECT
    holder_id,
    end_date,
    premium,
    'policy' AS origin
FROM policies
WHERE status = 'CANCELLED'
