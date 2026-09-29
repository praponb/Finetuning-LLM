from pyspark.sql import functions as F

result = df_stores.agg(F.max("opened_date").alias("last_opened_date"))
