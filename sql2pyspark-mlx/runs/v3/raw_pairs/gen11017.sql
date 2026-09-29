SELECT channel, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values FROM orders GROUP BY channel ORDER BY channel
