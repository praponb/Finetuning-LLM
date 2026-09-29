from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("city").isin(["London", "Singapore"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating")).otherwise(0)).alias("singapore_rating"),
        F.sum("rating").alias("total_rating"),
    )
)
