from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w_hist = Window.partitionBy("country").orderBy(F.col("start_date").desc(), "id")
w3 = Window.partitionBy("country").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc_nulls_first(), "id")

valid = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2024-05-17").cast("date"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.concat_ws("/", "status", "plan").alias("status_plan_key"),
        F.weekofyear("start_date").alias("start_date_weekofyear"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.country"), F.col("p.city"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.row_number().over(w_hist).alias("row_seq"),
        F.sum("monthly_fee").over(w3).alias("cumulative_monthly_fee"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

remaining_keys = (
    latest
    .select("id", "customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("plan") == "premium")
        .select("id", "customer_id")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

wide = (
    verified
    .groupBy("status", "customer_id")
    .pivot("country", ["DE", "TH", "SG"])
    .agg(F.avg("row_seq").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "status",
        "customer_id",
        F.col("DE_amount").alias("de_amount"),
        F.col("DE_n").alias("de_n"),
        F.col("TH_amount").alias("th_amount"),
        F.col("TH_n").alias("th_n"),
        F.col("SG_amount").alias("sg_amount"),
        F.col("SG_n").alias("sg_n"),
    )
)

result = (
    wide
    .select("status", "customer_id", "th_n", "sg_amount", "sg_n")
    .orderBy("status", "customer_id")
)
