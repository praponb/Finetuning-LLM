from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-03-29"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        (F.datediff(F.current_date(), "shipped_date") / 7).alias("weeks_since_shipped_date"),
    )
)

valid = (
    filtered
    .filter(F.col("delivered_date") >= F.lit("2023-03-08").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.col("weeks_since_shipped_date").cast("int").alias("weeks_since_shipped_date_int"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

enriched = (
    valid
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.discount"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.cost",
        "a.status",
        "a.weeks_since_shipped_date_int",
        "a.resolved_carrier",
        "a.discount",
        F.col("b.channel"),
        F.col("b.discount").alias("order_discount"),
    )
)

base = (
    with_dims
    .filter(F.col("cost").between(35, 52))
    .withColumn("discount_int", F.col("discount").cast("int"))
    .withColumn("cost_tier", F.when(F.col("cost") >= 98, "High").when(F.col("cost") >= 23, "Medium").otherwise("Low"))
)

base_shipments = (
    base
    .filter(F.col("status").isin(["RETURNED", "IN_TRANSIT", "LOST"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        "weeks_since_shipped_date_int",
        "resolved_carrier",
        "order_discount",
        "discount_int",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

result = (
    base_shipments
    .select(
        "id",
        "order_id",
        F.col("order_discount").alias("ORDER_DISCOUNT"),
        F.col("cost").alias("COST_VAL"),
    )
    .unpivot(
        ids=["id", "order_id"],
        values=["ORDER_DISCOUNT", "COST_VAL"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
)
