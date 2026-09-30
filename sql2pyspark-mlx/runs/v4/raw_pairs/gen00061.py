from pyspark.sql import functions as F

result = df_inventory.select("id", "quantity", "last_restock", "store_id")
