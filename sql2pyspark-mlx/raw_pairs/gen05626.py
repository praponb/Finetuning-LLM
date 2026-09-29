from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").asc_nulls_last(), "id")

latest = (
    df_inventory
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .groupBy(F.date_trunc("month", "last_restock").alias("activity_month"))
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
