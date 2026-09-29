select stores.region, count(inventory.id) as inventory_count from stores left join inventory on inventory.store_id = stores.id where stores.store_name like '%Downto%' group by stores.region
