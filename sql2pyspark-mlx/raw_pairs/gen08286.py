from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_recent = Window.partitionBy("order_id").orderBy("line_total")
w_grp = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") < 370))
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
)

metrics = (
    valid
    .select(
        "id",
        "quantity",
        "unit_price",
        F.last("quantity", ignorenulls=True).over(w).alias("last_known_quantity"),
        F.lead("unit_price", 1).over(w2).alias("next_unit_price"),
        F.rank().over(w_recent).alias("line_total_rank"),
        F.last("unit_price", ignorenulls=True).over(w_grp).alias("last_known_unit_price"),
    )
)

cleaned = (
    metrics
    .filter(F.col("unit_price").between(110, 160))
    .select(
        "id",
        "quantity",
        "unit_price",
        "next_unit_price",
        "line_total_rank",
        "last_known_unit_price",
        F.round("last_known_quantity", 1).alias("last_known_quantity_rounded"),
    )
)

base = (
    cleaned
    .filter(F.col("quantity").between(5, 6))
    .withColumn("last_known_unit_price_whole", F.ceil("last_known_unit_price"))
    .withColumn("next_unit_price_int", F.col("next_unit_price").cast("int"))
)

base_order_items = (
    base
    .filter(F.col("line_total_rank").isNotNull())
    .select(
        "id",
        "quantity",
        "unit_price",
        "next_unit_price",
        "last_known_unit_price",
        "last_known_quantity_rounded",
        "last_known_unit_price_whole",
        "next_unit_price_int",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

result = (
    base_order_items
    .groupBy(
        F.when(F.col("line_total") >= 81, "LARGE").otherwise("SMALL").alias("line_total_tier"),
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.count("*").alias("num_rows"),
        F.max("last_known_unit_price_whole").alias("max_last_known_unit_price_whole"),
    )
    .orderBy(F.col("quantity_bit_and").desc())
)
