from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "store_name",
        F.when(F.col("city").isin(["Bangkok", "Singapore"]), "Core").otherwise("Other").alias("city_group"),
    )
    .orderBy("city_group")
)
