from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("ship_country").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_recent = Window.partitionBy("ship_country").orderBy(F.col("created_at").desc(), "id")
w4 = Window.partitionBy("ship_country").orderBy("bulk_lines", "id")

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.size(F.filter("items", lambda li: li["qty"] >= 3)).alias("bulk_lines"),
        F.col("ship_to.country").alias("ship_country"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "created_at",
        "city_or_unknown",
        "ship_country",
        "has_premium_item",
        F.sum("bulk_lines").over(w_part).alias("cumulative_bulk_lines"),
        F.sum("bulk_lines").over(w2).alias("bulk_lines_sum_15m"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.sum("bulk_lines").over(w_part).alias("running_bulk_lines"),
        F.lag("bulk_lines", 1).over(w4).alias("prev_bulk_lines"),
    )
)
