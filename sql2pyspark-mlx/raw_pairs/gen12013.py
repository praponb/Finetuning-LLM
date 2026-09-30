from pyspark.sql import functions as F

result = df_products.select("cost", "product_name", "supplier_id")
