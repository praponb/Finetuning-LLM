from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("ship_city").orderBy(F.col("created_at").desc(), "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes")["device"].alias("device"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.last("device", ignorenulls=True).over(w).alias("last_known_device"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
