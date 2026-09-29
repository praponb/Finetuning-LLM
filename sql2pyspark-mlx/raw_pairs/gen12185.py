from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")

latest = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    latest
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.order_date") < "2024-08-01")
    .select(
        "a.id",
        "a.order_id",
        "a.shipped_date",
        "a.status",
        F.col("o.status").alias("order_status"),
    )
)

base = (
    joined
    .filter((F.col("shipped_date") > F.lit("2023-02-15").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "status",
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

base_shipments = (
    base
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.upper("status").alias("status_upper"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

valid = (
    base_shipments
    .filter(~F.col("status").isin(["IN_TRANSIT", "DELIVERED"]))
    .withColumn("status_lower", F.lower("status"))
)

result = valid.select("id", "status", "status_upper")
