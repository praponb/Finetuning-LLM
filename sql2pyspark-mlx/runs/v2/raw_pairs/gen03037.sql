SELECT SORT_ARRAY(COLLECT_SET(location)) location_list, COUNT(*) total_count FROM departments ORDER BY total_count DESC
