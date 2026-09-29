from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("supplier_name").isNotNull())

suppliers_stats = (
    base
    .groupBy("city")
    .agg(
        F.max("rating").alias("max_rating"),
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
    )
)

result = (
    suppliers_stats
    .orderBy("city")
    .limit(1)
)
