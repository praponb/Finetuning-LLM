from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id", "ship_country").orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("max_item_price").desc(), "id")

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attributes",
        F.col("ship_to.country").alias("ship_country"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "customer_id",
        "ship_country",
        "max_item_price",
        F.sum("max_item_price").over(w_part).alias("running_sum_max_item_price"),
        F.count("max_item_price").over(w_part).alias("running_count_max_item_price"),
        F.min("max_item_price").over(w_part).alias("running_min_max_item_price"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
