SELECT *, ROW_NUMBER() OVER (PARTITION BY browser, country ORDER BY started_at DESC NULLS LAST, id) AS row_num FROM sessions QUALIFY row_num = 1
