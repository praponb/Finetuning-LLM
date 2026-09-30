from pyspark.sql import functions as F

result = df_stores.select("sqft", F.round("sqft", 0).alias("sqft_rounded"))
