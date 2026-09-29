from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("months_delivered_date_to_shipped_date", "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_last())
w4 = Window.partitionBy(F.month("shipped_date")).orderBy("cost", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy("first_known_months_delivered_date_to_shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("running_sum_months_delivered_date_to_shipped_date_avg_2_prior_plus_running_count_months_delivered_date_to_shipped_date_avg_2_prior", "id").rowsBetween(-2, -1)

cleaned = (
    df_shipments
    .filter(F.col("cost") > 60)
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        F.round((F.datediff("shipped_date", "delivered_date") / 30), 1).alias("months_delivered_date_to_shipped_date"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

enriched = (
    cleaned
    .filter(F.year("shipped_date") >= 2024)
    .withColumn("next_cost", F.lead("cost", 2, 0).over(w_recent))
    .withColumn("order_id_max_cost", F.max("cost").over(w2))
    .withColumn("cost_percent_rank", F.percent_rank().over(w3))
    .withColumn("months_delivered_date_to_shipped_date_avg_2_prior", F.avg("months_delivered_date_to_shipped_date").over(w4))
    .withColumn("first_known_months_delivered_date_to_shipped_date", F.first("months_delivered_date_to_shipped_date", ignorenulls=True).over(w5))
)

with_windows = (
    enriched
    .select(
        "*",
        F.count("months_delivered_date_to_shipped_date_avg_2_prior").over(w_grp).alias("running_count_months_delivered_date_to_shipped_date_avg_2_prior"),
        F.avg("months_delivered_date_to_shipped_date_avg_2_prior").over(w_grp).alias("running_avg_months_delivered_date_to_shipped_date_avg_2_prior"),
        F.sum("months_delivered_date_to_shipped_date_avg_2_prior").over(w_grp).alias("running_sum_months_delivered_date_to_shipped_date_avg_2_prior"),
        F.avg("months_delivered_date_to_shipped_date").over(w_ordered).alias("months_delivered_date_to_shipped_date_avg_2"),
    )
)

enriched_shipments = (
    with_windows
    .withColumn("running_count_running_avg_months_delivered_date_to_shipped_date_avg_2_prior", F.count("running_avg_months_delivered_date_to_shipped_date_avg_2_prior").over(w8))
    .withColumn("running_max_running_avg_months_delivered_date_to_shipped_date_avg_2_prior", F.max("running_avg_months_delivered_date_to_shipped_date_avg_2_prior").over(w8))
    .withColumn("running_sum_running_avg_months_delivered_date_to_shipped_date_avg_2_prior", F.sum("running_avg_months_delivered_date_to_shipped_date_avg_2_prior").over(w8))
)

base = (
    enriched_shipments
    .filter(F.col("delivered_date") >= F.lit("2023-08-10").cast("date"))
    .withColumn("running_count_running_avg_months_delivered_date_to_shipped_date_avg_2_prior_whole", F.floor("running_count_running_avg_months_delivered_date_to_shipped_date_avg_2_prior"))
    .withColumn("running_sum_months_delivered_date_to_shipped_date_avg_2_prior_plus_running_count_months_delivered_date_to_shipped_date_avg_2_prior", (F.col("running_sum_months_delivered_date_to_shipped_date_avg_2_prior") + F.col("running_count_months_delivered_date_to_shipped_date_avg_2_prior")))
)

metrics = (
    base
    .withColumn("running_count_months_delivered_date_to_shipped_date_avg_2_prior_share", F.round((F.col("running_count_months_delivered_date_to_shipped_date_avg_2_prior") / F.sum("running_count_months_delivered_date_to_shipped_date_avg_2_prior").over(w2)), 4))
    .withColumn("order_id_max_cost_avg_2_prior", F.avg("order_id_max_cost").over(w9))
)

filtered = (
    metrics
    .filter((F.col("shipped_date") > F.lit("2023-06-03").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("running_count_running_avg_months_delivered_date_to_shipped_date_avg_2_prior_minus_running_sum_running_avg_months_delivered_date_to_shipped_date_avg_2_prior", (F.col("running_count_running_avg_months_delivered_date_to_shipped_date_avg_2_prior") - F.col("running_sum_running_avg_months_delivered_date_to_shipped_date_avg_2_prior")))
)

result = (
    filtered
    .select(
        "id",
        "shipped_date",
        "cost",
        "months_delivered_date_to_shipped_date",
        "resolved_carrier",
        "next_cost",
        "order_id_max_cost",
        "cost_percent_rank",
        "months_delivered_date_to_shipped_date_avg_2_prior",
    )
    .orderBy("order_id_max_cost")
)
