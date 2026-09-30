from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w_ordered = Window.partitionBy("product_id", "order_id")
w_grp = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("order_id").orderBy("product_id_min_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id", "order_id").orderBy(F.col("product_id_min_unit_price").desc_nulls_last())
w7 = Window.partitionBy("product_id").orderBy(F.col("product_id_min_unit_price_whole").desc_nulls_first())
w8 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w9 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("product_id").orderBy("first_known_quantity", "id").rowsBetween(-5, Window.currentRow)

deduped = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    deduped
    .withColumn("product_id_min_unit_price", F.min("unit_price").over(w_ordered))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w_grp))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w4))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w_grp))
)

base = (
    metrics
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.floor("product_id_min_unit_price").alias("product_id_min_unit_price_whole"),
    )
)

with_windows = (
    base
    .withColumn("first_known_corr_quantity_unit_price", F.first("corr_quantity_unit_price", ignorenulls=True).over(w5))
    .withColumn("product_id_min_unit_price_percent_rank", F.percent_rank().over(w6))
)

enriched = (
    with_windows
    .withColumn("product_id_min_unit_price_whole_percent_rank", F.percent_rank().over(w7))
    .withColumn("quantity_bucket", F.ntile(3).over(w8))
    .withColumn("running_product_id_min_unit_price_percent_rank", F.sum("product_id_min_unit_price_percent_rank").over(w9))
    .withColumn("quantity_avg_5", F.avg("quantity").over(w10))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("unit_price") >= 340, "HIGH").otherwise("LOW").alias("unit_price_tier"),
        F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        F.when(F.col("product_id_min_unit_price") >= 383, "HIGH").otherwise("LOW").alias("product_id_min_unit_price_tier"),
    )
    .agg(
        F.sum("product_id_min_unit_price_percent_rank").alias("total_product_id_min_unit_price_percent_rank"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
    )
)
