from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_country").orderBy(F.col("num_items").desc(), "id")
w2 = Window.partitionBy("ship_country").orderBy(F.col("num_items").desc())
w3 = Window.partitionBy("ship_country").orderBy("num_items", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("ship_country").orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.col("ship_to.city").alias("ship_city"),
        F.col("ship_to.country").alias("ship_country"),
        F.size("items").alias("num_items"),
    )
)

result = (
    unpacked
    .filter(F.size("coupon_codes") >= 2)
    .select(
        "id",
        "ship_country",
        "num_items",
        F.row_number().over(w).alias("row_seq"),
        F.rank().over(w2).alias("num_items_rank"),
        F.avg("num_items").over(w3).alias("num_items_avg_6"),
        F.last("num_items", ignorenulls=True).over(w4).alias("last_known_num_items"),
    )
)
