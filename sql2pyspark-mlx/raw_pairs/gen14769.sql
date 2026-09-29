SELECT
    id,
    service,
    context,
    COALESCE(context['version'], 'direct') AS version_or_default,
    CAST((status_code & 2) != 0 AS INT) AS flag_bit2
FROM app_logs;
