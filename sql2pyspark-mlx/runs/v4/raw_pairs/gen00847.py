from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

deduped = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .groupBy("order_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.avg("unit_price").alias("avg_unit_price_2"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .filter(F.col("distinct_order_ids") > 1)
    .orderBy(F.col("order_id").desc_nulls_first())
)
