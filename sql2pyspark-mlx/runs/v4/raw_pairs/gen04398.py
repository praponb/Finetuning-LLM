from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("num_items").desc(), "id")
w2 = Window.partitionBy("ship_country").orderBy("created_at", "id")
w3 = Window.orderBy("num_items", "id")
w4 = Window.partitionBy("ship_country").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        "attributes",
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
        F.size("items").alias("num_items"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("none")).alias("gift_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.lag("num_items", 2).over(w2).alias("prev_num_items"),
        F.ntile(3).over(w3).alias("num_items_bucket"),
        F.last("num_items", ignorenulls=True).over(w4).alias("last_known_num_items"),
    )
)
