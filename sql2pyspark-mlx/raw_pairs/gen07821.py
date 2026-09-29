from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_country")
w2 = Window.orderBy(F.col("num_items").asc_nulls_last())

flattened_attrs = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
        F.size("items").alias("num_items"),
        F.col("ship_to.country").alias("ship_country"),
        F.col("attributes")["device"].alias("device"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "coupon_codes",
        "has_device",
        "num_items",
        "ship_country",
        "device",
        F.stddev_samp("num_items").over(w).alias("ship_country_stddev_samp_num_items"),
        F.cume_dist().over(w2).alias("num_items_cume_dist"),
    )
)
