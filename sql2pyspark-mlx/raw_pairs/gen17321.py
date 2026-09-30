from pyspark.sql import functions as F

src = (
    df_shipments
    .filter((F.year("shipped_date") == 2025) & F.col("cost").between(36, 83))
    .withColumn("is_flash", F.when(F.col("carrier") == "Flash", 1).otherwise(0))
)

shipments_stats = (
    src
    .groupBy("status")
    .agg(
        (F.max("is_flash") - F.min("is_flash")).alias("is_flash_range"),
        F.max("is_flash").alias("max_is_flash"),
    )
)

result = (
    shipments_stats
    .orderBy("max_is_flash", "status")
    .limit(10)
)
