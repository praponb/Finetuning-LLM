from pyspark.sql import functions as F

result = df_inventory.select("quantity", "last_restock", "product_id")
