from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("product_id").asc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

result = (
    flattened
    .select(
        "id",
        "created_at",
        "product_id",
        "price",
        F.percent_rank().over(w).alias("product_id_percent_rank"),
        F.count("product_id").over(w2).alias("product_id_count_1h"),
        F.count("qty").over(w3).alias("qty_count_15m"),
    )
)
