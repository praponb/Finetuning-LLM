from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w_running = Window.partitionBy("product_id").orderBy("quantity", "id")

scored = (
    df_order_items
    .withColumn("prev_unit_price", F.lag("unit_price").over(w_recent))
    .withColumn("next_unit_price", F.lead("unit_price").over(w_recent))
    .withColumn("next_quantity", F.lead("quantity").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("quantity_bucket", F.ntile(5).over(w_running))
)

result = (
    scored
    .groupBy(
        F.when(F.col("quantity_bucket") >= 9, "LARGE").otherwise("SMALL").alias("quantity_bucket_tier"),
    )
    .agg(
        F.percentile_approx("quantity_bucket", 0.5).alias("quantity_bucket_p50"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.avg("prev_unit_price").alias("avg_prev_unit_price"),
        F.min("next_quantity").alias("min_next_quantity"),
    )
    .filter(F.col("min_next_quantity") < 1000000)
)
