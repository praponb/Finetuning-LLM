from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("carrier").orderBy("weeks_shipped_date_to_delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_first(), "id")
w4 = Window.partitionBy("carrier").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("running_sum_weeks_shipped_date_to_delivered_date", "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-05-27"))) & (~F.col("carrier").eqNullSafe(F.lit("FedEx"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        F.date_format("delivered_date", "yyyy-MM-dd").alias("delivered_date_label"),
        (F.datediff("delivered_date", "shipped_date") / 7).alias("weeks_shipped_date_to_delivered_date"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

metrics = (
    cleaned
    .filter(F.col("carrier").isin(["Flash", "Kerry"]))
    .select(
        "*",
        F.sum("weeks_shipped_date_to_delivered_date").over(w_part).alias("running_sum_weeks_shipped_date_to_delivered_date"),
        F.avg("weeks_shipped_date_to_delivered_date").over(w_part).alias("running_avg_weeks_shipped_date_to_delivered_date"),
        F.max("weeks_shipped_date_to_delivered_date").over(w_part).alias("running_max_weeks_shipped_date_to_delivered_date"),
        F.last("weeks_shipped_date_to_delivered_date", ignorenulls=True).over(w_ordered).alias("last_known_weeks_shipped_date_to_delivered_date"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.min("weeks_shipped_date_to_delivered_date").over(w4).alias("running_min_weeks_shipped_date_to_delivered_date"),
        F.avg("weeks_shipped_date_to_delivered_date").over(w4).alias("running_avg_weeks_shipped_date_to_delivered_date_2"),
        F.sum("weeks_shipped_date_to_delivered_date").over(w4).alias("running_sum_weeks_shipped_date_to_delivered_date_2"),
        F.max("weeks_shipped_date_to_delivered_date").over(w4).alias("running_max_weeks_shipped_date_to_delivered_date_2"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "delivered_date_label",
        "weeks_shipped_date_to_delivered_date",
        "carrier_clean",
        "running_avg_weeks_shipped_date_to_delivered_date",
        "last_known_weeks_shipped_date_to_delivered_date",
        "running_min_weeks_shipped_date_to_delivered_date",
        "running_avg_weeks_shipped_date_to_delivered_date_2",
        "running_sum_weeks_shipped_date_to_delivered_date_2",
        F.lead("last_known_weeks_shipped_date_to_delivered_date").over(w5).alias("next_last_known_weeks_shipped_date_to_delivered_date"),
        F.lag("last_known_weeks_shipped_date_to_delivered_date").over(w5).alias("prev_last_known_weeks_shipped_date_to_delivered_date"),
        F.lead("running_sum_weeks_shipped_date_to_delivered_date").over(w5).alias("next_running_sum_weeks_shipped_date_to_delivered_date"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

result = (
    with_windows
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date_label",
        "weeks_shipped_date_to_delivered_date",
        "last_known_weeks_shipped_date_to_delivered_date",
        "running_min_weeks_shipped_date_to_delivered_date",
        "running_sum_weeks_shipped_date_to_delivered_date_2",
        "prev_last_known_weeks_shipped_date_to_delivered_date",
        F.when(F.col("carrier").rlike("^(UPS|Flash)$"), "EXTENDED").when(F.col("carrier").rlike("^(DHL|FedEx)$"), "GROUP_B").otherwise("CORE").alias("carrier_zone"),
        F.col("avg_payment_amount"),
    )
)
