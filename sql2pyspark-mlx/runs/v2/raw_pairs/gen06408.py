from pyspark.sql import functions as F

result = df_stores.select("id", "sqft", F.round("sqft", 2).alias("sqft_rounded"))
