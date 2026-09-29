from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.max("quantity").alias("max_order_item_quantity"))

filtered = (
    df_shipments
    .filter(F.col("shipped_date") > F.to_date(F.lit("2023-09-23")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.upper("carrier").alias("carrier_upper"),
        F.when(F.col("carrier") == "DHL", 1).otherwise(0).alias("is_dhl"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "carrier_upper",
        "resolved_carrier",
    )
)

enriched = (
    ranked
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.status").alias("order_status"))
)

cleaned = (
    enriched
    .filter((F.col("delivered_date") >= F.lit("2023-05-26").cast("date")) & F.col("order_id").isNull())
    .withColumn("order_status_upper", F.upper("order_status"))
    .withColumn("order_status_filled", F.coalesce("order_status", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        "carrier_upper",
        "resolved_carrier",
        "order_status",
        F.when(F.col("order_status").rlike("^(PENDING|SHIPPED)$"), "GROUP_A").when(F.col("order_status").rlike("^(COMPLETED|CANCELLED)$"), "GROUP_B").otherwise("PRIORITY").alias("order_status_zone"),
        F.col("max_order_item_quantity"),
    )
    .orderBy(F.col("id").desc_nulls_last(), "order_status")
)
