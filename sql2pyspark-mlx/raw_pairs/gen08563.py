from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_items", "id")
w2 = Window.partitionBy("ship_country").orderBy(F.col("num_items").desc_nulls_first())
w3 = Window.partitionBy("ship_country").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "ship_to",
        F.size("items").alias("num_items"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("unknown")).alias("device_or_default"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.col("ship_to.country").alias("ship_country"),
    )
)

result = (
    parsed
    .select(
        "id",
        "created_at",
        "ship_country",
        F.row_number().over(w).alias("row_seq"),
        F.rank().over(w2).alias("num_items_rank"),
        F.sum("num_items").over(w3).alias("cumulative_num_items"),
    )
)
