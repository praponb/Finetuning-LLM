from pyspark.sql import functions as F

result = df_stores.select("id", "region", F.col("sqft").cast("double").alias("sqft_num"))
