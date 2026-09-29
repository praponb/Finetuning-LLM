from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") >= "2025-06-19") & ~F.col("city").isin(["Bangkok", "Singapore"]))
    .select(
        "id",
        "store_name",
        "region",
        F.coalesce("store_name", F.lit("Unknown")).alias("store_name_filled"),
        F.when(F.col("city").isin(["Bangkok", "New York"]), "Priority").otherwise("Other").alias("city_group"),
    )
    .orderBy("region")
)
