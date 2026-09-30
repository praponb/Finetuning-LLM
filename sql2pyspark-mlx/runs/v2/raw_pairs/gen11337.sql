select merchant, count(*) as total_count from transactions where merchant not in ('Shell', '7-Eleven', 'Amazon') and is_flagged = false group by merchant having count(*) < 4 order by merchant asc
