from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w_ordered = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy("first_known_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("first_known_budget").desc(), "id")
w5 = Window.orderBy("first_known_budget", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("location").orderBy("event_seq", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("location").orderBy(F.col("first_known_budget").desc(), "id")
w8 = Window.partitionBy("location").orderBy(F.col("first_known_budget_bucket").desc(), "id")
w9 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("location")
w11 = Window.orderBy("first_known_budget_bucket", "id")

enriched = (
    df_departments
    .select(
        "id",
        "location",
        F.row_number().over(w).alias("event_seq"),
        F.first("budget", ignorenulls=True).over(w_ordered).alias("first_known_budget"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "location",
        F.sum("first_known_budget").over(w3).alias("cumulative_first_known_budget"),
        F.ntile(5).over(w4).alias("first_known_budget_bucket"),
        F.avg("event_seq").over(w5).alias("event_seq_avg_3"),
        F.avg("event_seq").over(w6).alias("event_seq_avg_3_prior"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .filter(F.col("seq_no") >= 5)
    .select(
        "*",
        F.first("seq_no", ignorenulls=True).over(w9).alias("first_known_seq_no"),
        F.count("cumulative_first_known_budget").over(w_hist).alias("location_count_cumulative_first_known_budget"),
        F.lead("cumulative_first_known_budget", 2).over(w11).alias("next_cumulative_first_known_budget"),
        F.sum("event_seq_avg_3").over(w_hist).alias("location_sum_event_seq_avg_3"),
    )
)

result = (
    metrics
    .select(
        "id",
        "location",
        "cumulative_first_known_budget",
        "first_known_budget_bucket",
        "event_seq_avg_3",
        "seq_no",
        "first_known_seq_no",
        "location_count_cumulative_first_known_budget",
        "next_cumulative_first_known_budget",
        F.when(F.col("location").rlike("^(Singapore|Berlin)$"), "PRIORITY").when(F.col("location").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("location_zone"),
    )
)
