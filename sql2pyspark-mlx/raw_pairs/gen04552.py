from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("status").orderBy(F.col("shipped_date").desc_nulls_first(), "id")
w4 = Window.partitionBy("carrier").orderBy("shipped_date_plus_months", "id")
w5 = Window.partitionBy(F.month("delivered_date"), "carrier")

filtered = (
    df_shipments
    .filter(F.col("order_id").isin([33, 39]))
    .select(
        "*",
        F.add_months("shipped_date", 1).alias("shipped_date_plus_months"),
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
    )
)

metrics = (
    filtered
    .filter(F.col("order_id").isin([2, 30]))
    .withColumn("cost_avg_2", F.avg("cost").over(w))
    .withColumn("cost_avg_6_prior", F.avg("cost").over(w2))
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "carrier_status_key",
        "cost_avg_6_prior",
        F.lag("cost_avg_6_prior").over(w4).alias("prev_cost_avg_6_prior"),
        F.lag("cost_avg_2").over(w4).alias("prev_cost_avg_2"),
        F.lead("cost_avg_2").over(w4).alias("next_cost_avg_2"),
        F.row_number().over(w4).alias("seq_no"),
        F.round((F.col("cost_avg_2") / F.sum("cost_avg_2").over(w5)), 4).alias("cost_avg_2_share"),
    )
)

result = (
    enriched
    .withColumn("cost_tier", F.when(F.col("cost") >= 78, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["status", "cost_tier"], ["status"], []], "status", "cost_tier")
    .agg(
        F.percentile_approx("prev_cost_avg_2", 0.95).alias("prev_cost_avg_2_p95"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("cost_per_delivered"),
        F.count("*").alias("row_count"),
        F.max("seq_no").alias("max_seq_no"),
    )
    .filter(F.col("max_seq_no") < 1000000)
)
