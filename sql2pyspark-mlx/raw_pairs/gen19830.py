from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("monthly_fee_whole").desc(), "id")
w_ordered = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("customer_id", "status").orderBy(F.col("end_date").desc(), "id")
w5 = Window.partitionBy("status").orderBy(F.col("row_seq").asc_nulls_last(), "id")
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("status").orderBy(F.col("row_seq").desc_nulls_first())

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.floor("monthly_fee").alias("monthly_fee_whole"))
)

metrics = (
    valid
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w))
    .withColumn("monthly_fee_whole_bucket", F.ntile(4).over(w2))
    .withColumn("last_known_monthly_fee_whole", F.last("monthly_fee_whole", ignorenulls=True).over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_hist))
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    latest
    .select("plan", "status")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select("plan", "status")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "last_known_monthly_fee",
        "last_known_monthly_fee_whole",
        "row_seq",
        "row_num",
        F.count("last_known_monthly_fee_whole").over(w6).alias("status_count_last_known_monthly_fee_whole"),
        F.rank().over(w7).alias("row_seq_rank"),
    )
)

totals = (
    with_windows
    .withColumn("quarter_start", F.date_trunc("quarter", "end_date"))
    .groupingSets(
        [["status", "quarter_start"], ["status"], []],
        "status",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.round(F.avg("last_known_monthly_fee"), 2).alias("avg_last_known_monthly_fee"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    totals
    .select(
        "status",
        "quarter_start",
        "avg_last_known_monthly_fee",
        "plan_list",
        "num_plans",
    )
)
