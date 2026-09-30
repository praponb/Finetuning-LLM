from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w2 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc(), "id")
w6 = Window.orderBy(F.col("shipping_fee").desc())
w7 = Window.orderBy("first_known_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy("first_known_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-03-31").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "shipping_fee",
        F.when(F.col("shipping_fee") >= 32, "High").when(F.col("shipping_fee") >= 8, "Medium").otherwise("Low").alias("shipping_fee_band"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "customer_id",
        "store_id",
        "shipping_fee",
        "shipping_fee_band",
        F.lead("shipping_fee", 1).over(w_running).alias("next_shipping_fee"),
        F.last("shipping_fee", ignorenulls=True).over(w2).alias("last_known_shipping_fee"),
        F.min("shipping_fee").over(w3).alias("status_min_shipping_fee"),
        F.first("shipping_fee", ignorenulls=True).over(w4).alias("first_known_shipping_fee"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

with_windows_orders = (
    with_windows
    .filter(F.col("last_known_shipping_fee").isNotNull())
    .select(
        "*",
        F.dense_rank().over(w6).alias("shipping_fee_dense_rank"),
        F.sum("last_known_shipping_fee").over(w7).alias("cumulative_last_known_shipping_fee"),
    )
)

with_dims = (
    with_windows_orders
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.next_shipping_fee",
        "e.last_known_shipping_fee",
        "e.status_min_shipping_fee",
        "e.first_known_shipping_fee",
        "e.cumulative_last_known_shipping_fee",
        F.col("c.city"),
        F.col("c.name"),
    )
)

result = (
    with_dims
    .select(
        "*",
        F.avg("status_min_shipping_fee").over(w_part).alias("running_avg_status_min_shipping_fee"),
        F.min("status_min_shipping_fee").over(w_part).alias("running_min_status_min_shipping_fee"),
        F.max("status_min_shipping_fee").over(w_part).alias("running_max_status_min_shipping_fee"),
        F.sum("status_min_shipping_fee").over(w_part).alias("running_sum_status_min_shipping_fee"),
    )
)
