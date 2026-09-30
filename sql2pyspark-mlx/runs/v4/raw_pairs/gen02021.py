from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last(), "id")

latest = (
    df_inventory
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("year", "last_restock").alias("year_start"),
        F.when(F.col("quantity") >= 197, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
        "product_id",
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)
