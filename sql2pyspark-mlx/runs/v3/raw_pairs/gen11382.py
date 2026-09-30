from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "region",
        F.date_add("opened_date", 1).alias("opened_date_plus_1d"),
        F.when(F.col("region").isin(["Central", "North"]), "Tier 1").otherwise("Other").alias("region_group"),
    )
)
