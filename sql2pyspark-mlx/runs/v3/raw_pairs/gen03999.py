from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.upper(F.trim("supplier_name")) == "PACIFIC WHOLESALE")

suppliers_stats = (
    recent_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)

result = suppliers_stats.orderBy("avg_rating")
