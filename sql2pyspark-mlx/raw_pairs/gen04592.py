from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price_filled").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity")
w_running = Window.orderBy("unit_price", "id").rowsBetween(-2, -1)

valid = (
    df_order_items
    .filter(F.col("quantity") == 6)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

result = (
    valid
    .filter(F.col("unit_price").between(340, 390))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_filled",
        F.ntile(4).over(w).alias("unit_price_filled_bucket"),
        F.percent_rank().over(w2).alias("quantity_percent_rank"),
        F.avg("quantity").over(w_running).alias("quantity_avg_2_prior"),
    )
)
