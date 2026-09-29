from pyspark.sql import functions as F

result = df_suppliers.agg(F.countDistinct("country").alias("country_count"))
