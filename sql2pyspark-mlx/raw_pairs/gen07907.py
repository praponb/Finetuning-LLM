from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.orderBy(F.col("quantity").desc_nulls_first())

enriched = (
    df_inventory
    .withColumn("quantity_bucket", F.ntile(10).over(w))
    .withColumn("quantity_rank", F.rank().over(w2))
)

result = (
    enriched
    .groupBy(F.date_trunc("week", "last_restock").alias("week_start"))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.percentile_approx("quantity_bucket", [0.25, 0.5, 0.75]).alias("quantity_bucket_quartiles"),
        F.round(F.avg("quantity_bucket"), 2).alias("avg_quantity_bucket"),
        F.avg("quantity_bucket").alias("avg_quantity_bucket_2"),
        F.round(F.avg("quantity_bucket"), 2).alias("avg_quantity_bucket_3"),
    )
)
