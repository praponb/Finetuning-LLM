from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & ~F.col("carrier").isin(["FedEx", "DHL", "Flash"]))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "status",
        F.concat("status", F.lit("/"), "carrier").alias("status_carrier"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_filled"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isNotNull())
    .select(
        "src.id",
        "src.order_id",
        "src.shipped_date",
        "src.status",
        "src.status_carrier",
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
    )
)

deduped = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    deduped
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.id", "a.order_id", "a.shipped_date", "a.status", F.col("p.discount"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.status").alias("order_status"),
        F.col("p.discount").alias("order_discount"),
    )
)

result = (
    enriched
    .select("id", "order_id", "discount", "order_discount")
    .unpivot(
        ids=["id", "order_id"],
        values=["discount", "order_discount"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
