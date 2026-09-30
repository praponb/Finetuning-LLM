from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Tokyo", "Chiang Mai"]))
    .select(
        "*",
        F.when(F.col("rating") >= 4.2, "High").when(F.col("rating") >= 1.9, "Medium").otherwise("Low").alias("rating_band"),
    )
)

result = (
    cleaned
    .groupBy("country")
    .pivot("city", ["New York", "Berlin"])
    .agg(F.avg("rating"))
    .select(
        "country",
        F.col("New York").alias("new_york"),
        F.col("Berlin").alias("berlin"),
    )
)
