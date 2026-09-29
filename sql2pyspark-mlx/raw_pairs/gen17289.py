from pyspark.sql import functions as F

result = df_products.select("id", "brand", "supplier_id", "category")
