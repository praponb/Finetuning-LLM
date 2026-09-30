from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id").orderBy("greatest_quantity_unit_price", "id")
w2 = Window.orderBy("greatest_quantity_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("next_quantity_str").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("order_id").isin([26, 27, 30, 35]))
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
)

filtered_order_items = (
    filtered
    .filter(F.col("quantity") <= 3)
    .select(
        "id",
        "product_id",
        "quantity",
        "greatest_quantity_unit_price",
        (F.col("unit_price") / F.nullif("greatest_quantity_unit_price", F.lit(0))).alias("unit_price_per_greatest_quantity_unit_price"),
    )
)

with_windows = (
    filtered_order_items
    .select(
        "id",
        "greatest_quantity_unit_price",
        "unit_price_per_greatest_quantity_unit_price",
        F.lead("quantity").over(w_part).alias("next_quantity"),
        F.lag("quantity").over(w_part).alias("prev_quantity"),
        F.lead("unit_price_per_greatest_quantity_unit_price").over(w_part).alias("next_unit_price_per_greatest_quantity_unit_price"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

valid = (
    with_windows
    .filter(F.col("next_unit_price_per_greatest_quantity_unit_price") > 76)
    .select(
        "id",
        "greatest_quantity_unit_price",
        "next_quantity",
        "prev_quantity",
        "next_unit_price_per_greatest_quantity_unit_price",
        F.col("next_quantity").cast("double").alias("next_quantity_str"),
    )
)

result = (
    valid
    .filter(F.col("next_quantity_str").between(47, 61))
    .select(
        "*",
        F.first("next_unit_price_per_greatest_quantity_unit_price", ignorenulls=True).over(w2).alias("first_known_next_unit_price_per_greatest_quantity_unit_price"),
        F.ntile(3).over(w3).alias("next_quantity_str_bucket"),
    )
)
