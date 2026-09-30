from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .withColumn("supplier_name_city_key", F.concat_ws("/", "supplier_name", "city"))
    .withColumn("rating_whole", F.floor("rating"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("N/A")))
)

result = (
    cleaned
    .groupBy("country")
    .pivot("city", ["Berlin", "New York"])
    .agg(F.max("rating_whole"))
    .select(
        "country",
        F.col("Berlin").alias("berlin"),
        F.col("New York").alias("new_york"),
    )
)
