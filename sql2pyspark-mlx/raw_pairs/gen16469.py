from pyspark.sql import functions as F

result = (
    df_stores
    .select("city", "sqft", F.round((F.col("sqft") * 1.1), 2).alias("adjusted_sqft"))
)
