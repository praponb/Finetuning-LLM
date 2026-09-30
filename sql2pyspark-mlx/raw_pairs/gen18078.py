from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("ship_city").orderBy("num_items", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("num_items", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("ship_city").orderBy(F.col("num_items").desc(), "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        F.col("ship_to.city").alias("ship_city"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("unknown")).alias("gift_or_default"),
        F.size("items").alias("num_items"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "ship_city",
        "gift_or_default",
        F.sum("num_items").over(w).alias("cumulative_num_items"),
        F.avg("num_items").over(w_ordered).alias("num_items_avg_2"),
        F.avg("num_items").over(w3).alias("num_items_avg_3"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.row_number().over(w_grp).alias("seq_no_2"),
    )
)
