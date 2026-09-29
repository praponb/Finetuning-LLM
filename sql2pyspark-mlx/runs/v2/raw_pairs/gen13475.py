from pyspark.sql import functions as F

result = df_inventory.select("id", "store_id", "last_restock", "product_id")
