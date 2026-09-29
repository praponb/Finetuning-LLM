from pyspark.sql import functions as F

result = df_suppliers.orderBy(F.col("rating").asc())
