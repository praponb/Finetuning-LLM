from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc(), "id")

deduped = (
    df_inventory
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("quantity") >= 251, "HIGH").otherwise("LOW").alias("quantity_tier"),
        F.date_trunc("month", "last_restock").alias("activity_month"),
        "product_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.max("quantity").alias("max_quantity"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
