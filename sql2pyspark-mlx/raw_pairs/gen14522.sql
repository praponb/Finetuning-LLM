SELECT
    id,
    log_ts,
    context['version'] AS version,
    COALESCE(context['version'], 'direct') AS version_or_default,
    COALESCE(context['region'], 'none') AS region_or_default
FROM app_logs;
