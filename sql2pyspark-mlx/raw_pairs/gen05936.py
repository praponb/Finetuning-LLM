from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-6, -1)
w_part = Window.partitionBy("method")
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.date_trunc("MONTH", "prev_paid_at"), "order_id").orderBy("amount_avg_6_prior", "id")
w7 = Window.partitionBy("order_id")
w8 = Window.partitionBy(F.month("paid_at"), "order_id").orderBy("island_id", "id")

scored = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        F.avg("amount").over(w).alias("amount_avg_6_prior"),
        F.avg("amount").over(w_part).alias("method_avg_amount"),
    )
)

with_prev = scored.withColumn("prev_paid_at", F.lag("paid_at").over(w3))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_burst").over(w4))

cleaned = (
    islands
    .filter(F.col("paid_at") >= F.lit("2023-06-27").cast("date"))
    .select(
        "*",
        (F.col("is_new_burst") + F.col("island_id")).alias("is_new_burst_plus_island_id"),
        (F.col("island_id") / F.nullif("is_new_burst", F.lit(0))).alias("island_id_per_is_new_burst"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "amount_avg_6_prior",
        "method_avg_amount",
        "prev_paid_at",
        "is_new_burst",
        "is_new_burst_plus_island_id",
        "island_id_per_is_new_burst",
        F.first("amount_avg_6_prior", ignorenulls=True).over(w5).alias("first_known_amount_avg_6_prior"),
        F.lead("amount_avg_6_prior", 1).over(w6).alias("next_amount_avg_6_prior"),
        F.avg("island_id").over(w7).alias("order_id_avg_island_id"),
        F.row_number().over(w8).alias("seq_no"),
    )
)

cleaned_payments = (
    enriched
    .filter(F.col("is_new_burst") < 1)
    .select(
        "*",
        (F.col("next_amount_avg_6_prior") - F.col("is_new_burst_plus_island_id")).alias("next_amount_avg_6_prior_minus_is_new_burst_plus_island_id"),
    )
)

result = (
    cleaned_payments
    .select("id", "is_new_burst_plus_island_id", "island_id_per_is_new_burst")
    .unpivot(
        ids=["id"],
        values=["is_new_burst_plus_island_id", "island_id_per_is_new_burst"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
