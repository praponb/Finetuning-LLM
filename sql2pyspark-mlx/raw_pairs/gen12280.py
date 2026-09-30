from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("status", "order_id").orderBy("shipped_date", "id")

base = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-05-31").cast("date")) & (F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select(
        "*",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.store_id").isNotNull())
    .select(
        "e.*",
        F.col("d.order_date"),
        F.col("d.status").alias("order_status"),
        F.col("d.discount"),
    )
)

enriched_shipments = (
    enriched
    .select(
        "id",
        "carrier",
        "cost",
        "carrier_filled",
        "order_date",
        "discount",
        F.last("cost", ignorenulls=True).over(w).alias("last_known_cost"),
        F.ntile(4).over(w2).alias("cost_bucket"),
        F.lead("cost", 1).over(w3).alias("next_cost"),
    )
)

filtered = (
    enriched_shipments
    .filter(~F.col("carrier").eqNullSafe(F.lit("Flash")))
    .select(
        "id",
        "carrier",
        "cost",
        "carrier_filled",
        "order_date",
        "discount",
        "last_known_cost",
        "cost_bucket",
        (F.col("next_cost") / F.nullif("discount", F.lit(0))).alias("next_cost_per_discount"),
    )
)

result = (
    filtered
    .select(
        "id",
        "carrier",
        "next_cost_per_discount",
        "cost",
        "discount",
        "last_known_cost",
    )
    .unpivot(
        ids=["id", "carrier"],
        values=["next_cost_per_discount", "cost", "discount", "last_known_cost"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
