SELECT
    id,
    merchant,
    IF(is_flagged, 'Yes', 'No') is_flagged_label,
    CAST(is_flagged AS INT) is_flagged_flag
FROM transactions
