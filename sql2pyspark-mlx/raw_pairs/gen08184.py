from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((~F.col("carrier").eqNullSafe(F.lit("Flash"))) & (F.col("status") != "RETURNED"))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

result = (
    base
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.shipped_date",
        "a.cost",
        "a.status",
        "a.resolved_carrier",
        F.col("d.amount"),
        F.col("d.channel"),
    )
)
