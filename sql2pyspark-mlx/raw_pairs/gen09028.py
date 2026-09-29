from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("order_id", "status")
w_grp = Window.orderBy("cost_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("WEEK", "shipped_date"), "status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("cost_avg_5_prior").asc_nulls_last())
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

metrics = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        F.avg("cost").over(w).alias("cost_avg_5_prior"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

metrics_shipments = (
    metrics
    .select(
        "*",
        F.sum("cost_share").over(w_grp).alias("running_cost_share"),
        F.sum("cost_avg_5_prior").over(w4).alias("cumulative_cost_avg_5_prior"),
        F.sum("cost_avg_5_prior").over(w5).alias("cumulative_cost_avg_5_prior_2"),
    )
)

remaining_keys = (
    metrics_shipments
    .select("order_id")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date") >= "2024-09-01")
        .select("order_id")
    )
)

kept_rows = (
    metrics_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = kept_rows.select("*", F.cume_dist().over(w6).alias("cost_avg_5_prior_cume_dist"))

kept_keys = (
    enriched
    .select("id", "status")
    .intersectAll(
        df_shipments
        .filter(F.col("cost") >= 91)
        .select("id", "status")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        "cost_avg_5_prior",
        "cost_share",
        "running_cost_share",
        "cumulative_cost_avg_5_prior",
        "cumulative_cost_avg_5_prior_2",
        F.when(F.col("status").rlike("^(LOST|DELIVERED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(IN_TRANSIT|RETURNED)$"), "GROUP_B").otherwise("CORE").alias("status_zone"),
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("status_zone").desc(), "cumulative_cost_avg_5_prior_2")
)
