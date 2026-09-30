select src.*, p.product_name from inventory src left join products p on src.product_id = p.id
