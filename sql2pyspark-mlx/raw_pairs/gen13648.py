from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "ship_to",
        "attributes",
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "customer_id",
        "max_item_price",
        "city_or_unknown",
        F.sum("max_item_price").over(w).alias("running_sum_max_item_price"),
        F.min("max_item_price").over(w).alias("running_min_max_item_price"),
        F.max("max_item_price").over(w).alias("running_max_max_item_price"),
        F.avg("max_item_price").over(w).alias("running_avg_max_item_price"),
        F.sum("max_item_price").over(w2).alias("cumulative_max_item_price"),
    )
)
