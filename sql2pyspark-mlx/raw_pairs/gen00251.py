from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded_bucket").asc_nulls_last(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("first_known_quantity_rounded").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("adjusted_quantity_rounded_bucket").desc())
w6 = Window.orderBy("adjusted_quantity_rounded_bucket", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isin([22, 24, 33]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round("quantity", 1).alias("quantity_rounded"),
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

enriched = (
    cleaned
    .filter(F.col("quantity_rounded") > 49)
    .select(
        "id",
        "product_id",
        F.first("quantity_rounded", ignorenulls=True).over(w).alias("first_known_quantity_rounded"),
        F.ntile(5).over(w_running).alias("quantity_rounded_bucket"),
    )
)

valid = (
    enriched
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_rounded_bucket_rounded", F.round("quantity_rounded_bucket", 1))
)

cleaned_order_items = (
    valid
    .filter(F.col("first_known_quantity_rounded").between(36, 79))
    .withColumn("first_known_quantity_rounded_level", F.when(F.col("first_known_quantity_rounded") >= 69, "High").when(F.col("first_known_quantity_rounded") >= 27, "Medium").otherwise("Low"))
)

filtered = (
    cleaned_order_items
    .filter(F.col("quantity_rounded_bucket") == 5)
    .select(
        "*",
        (F.col("quantity_rounded_bucket_rounded") - F.col("quantity_rounded_bucket")).alias("quantity_rounded_bucket_rounded_minus_quantity_rounded_bucket"),
        F.round((F.col("quantity_rounded_bucket") * 0.9), 2).alias("adjusted_quantity_rounded_bucket"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "product_id",
        "quantity_rounded_bucket",
        "quantity_rounded_bucket_rounded",
        "first_known_quantity_rounded_level",
        F.ntile(5).over(w4).alias("first_known_quantity_rounded_bucket"),
        F.rank().over(w5).alias("adjusted_quantity_rounded_bucket_rank"),
        F.lag("quantity_rounded_bucket", 2).over(w6).alias("prev_quantity_rounded_bucket"),
    )
)
