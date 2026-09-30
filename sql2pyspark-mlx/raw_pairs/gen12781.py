from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy("qty", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "pos",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

result = (
    exploded
    .select(
        "id",
        "customer_id",
        "pos",
        "product_id",
        "qty",
        "price",
        F.max("qty").over(w).alias("running_max_qty"),
        F.avg("qty").over(w).alias("running_avg_qty"),
        F.min("qty").over(w).alias("running_min_qty"),
    )
)
