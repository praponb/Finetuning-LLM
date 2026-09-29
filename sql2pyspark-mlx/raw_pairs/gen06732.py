from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")
w3 = Window.orderBy("unit_price", "id")
w4 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_order_items
    .filter(F.col("quantity").between(6, 8))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.ntile(10).over(w3).alias("unit_price_bucket"),
        F.first("unit_price", ignorenulls=True).over(w4).alias("first_known_unit_price"),
    )
)

result = (
    enriched
    .withColumn("unit_price_bucket_tier", F.when(F.col("unit_price_bucket") >= 8, "PREMIUM").otherwise("STANDARD"))
    .rollup("order_id", "unit_price_bucket_tier")
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.min("unit_price_bucket").alias("min_unit_price_bucket"),
        F.round(F.avg("first_known_unit_price"), 2).alias("avg_first_known_unit_price"),
        F.grouping("order_id").alias("grouping_level"),
    )
)
