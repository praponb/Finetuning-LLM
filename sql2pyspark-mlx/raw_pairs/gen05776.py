from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("shipped_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status")
w_hist = Window.partitionBy("order_id", "carrier").orderBy("last4_max_cost", "id")

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-03-31").cast("date")) & (F.col("delivered_date") <= "2024-08-23"))
    .withColumn("carrier_status", F.concat("carrier", F.lit(" "), "status"))
)

enriched = (
    filtered
    .select(
        "*",
        F.sum("cost").over(w).alias("last4_sum_cost"),
        F.max("cost").over(w).alias("last4_max_cost"),
        F.count("cost").over(w).alias("last4_count_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.lag("cost_share").over(w_hist).alias("prev_cost_share"),
        F.lead("cost_share").over(w_hist).alias("next_cost_share"),
        F.lead("cost").over(w_hist).alias("next_cost"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.status").alias("order_status"),
        F.col("b.channel"),
        F.col("b.discount"),
    )
)

filtered_shipments = (
    with_dims
    .filter((F.col("shipped_date") > F.lit("2023-07-27").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("DHL"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        "carrier_status",
        "last4_sum_cost",
        "last4_max_cost",
        "last4_count_cost",
        "cost_share",
        "prev_cost_share",
        "next_cost",
        "channel",
        "discount",
        F.round("discount", 2).alias("discount_rounded"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

result = (
    filtered_shipments
    .select(
        "id",
        "carrier",
        F.col("cost").alias("COST_VAL"),
        F.col("next_cost").alias("NEXT_COST"),
        F.col("last4_max_cost").alias("LAST4_MAX_COST_VAL"),
        F.col("discount_rounded").alias("DISCOUNT_ROUNDED"),
    )
    .unpivot(
        ids=["id", "carrier"],
        values=["COST_VAL", "NEXT_COST", "LAST4_MAX_COST_VAL", "DISCOUNT_ROUNDED"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
