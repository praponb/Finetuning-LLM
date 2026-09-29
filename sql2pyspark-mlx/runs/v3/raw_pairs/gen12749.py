from pyspark.sql import functions as F

result = df_products.filter(F.col("supplier_id").isNull() & F.col("cost").between(370, 460))
