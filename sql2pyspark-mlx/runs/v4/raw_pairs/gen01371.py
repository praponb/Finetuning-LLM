from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("delivered_date", "id")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("carrier").orderBy("cumulative_days_since_shipped_date", "id")
w6 = Window.partitionBy("order_id").orderBy(F.col("cumulative_days_since_shipped_date").desc(), "id")
w7 = Window.partitionBy("order_id", "status")
w8 = Window.partitionBy("carrier").orderBy("cost_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
        F.when(F.col("carrier").isin(["DHL", "UPS"]), "Tier 1").otherwise("Other").alias("carrier_group"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.sum("days_since_shipped_date").over(w2).alias("cumulative_days_since_shipped_date"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

enriched = (
    scored
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w4)), 4))
    .withColumn("prev_cost", F.lag("cost", 1).over(w5))
    .withColumn("cumulative_days_since_shipped_date_bucket", F.ntile(3).over(w6))
)

enriched_shipments = (
    enriched
    .select(
        "*",
        F.min("prev_cost").over(w7).alias("order_id_min_prev_cost"),
        F.first("cumulative_days_since_shipped_date", ignorenulls=True).over(w8).alias("first_known_cumulative_days_since_shipped_date"),
    )
)

result = (
    enriched_shipments
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "days_since_shipped_date",
        "carrier_group",
        F.when(F.col("status").rlike("^(IN_TRANSIT|LOST)$"), "GROUP_B").when(F.col("status").rlike("^(DELIVERED|RETURNED)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("status_zone"),
        F.col("sum_shipment_cost"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
