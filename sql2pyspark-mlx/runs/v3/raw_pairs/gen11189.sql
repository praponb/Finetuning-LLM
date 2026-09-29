select id, supplier_name, coalesce(supplier_name, 'Unknown') as supplier_name_filled
from suppliers;
