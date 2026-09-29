from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating")).otherwise(0)).alias("singapore_rating"),
    )
)
