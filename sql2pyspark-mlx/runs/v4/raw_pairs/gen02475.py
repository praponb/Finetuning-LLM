from pyspark.sql import functions as F

base = df_products.filter(F.col("category") == "Fashion")

agg = (
    base
    .groupBy("brand")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.max("launch_date").alias("latest_launch_date"),
    )
)

result = (
    agg
    .select("brand", "min_launch_date")
    .orderBy(F.col("brand").desc(), "min_launch_date")
)
