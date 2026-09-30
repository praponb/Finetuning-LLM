from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("end_date", "id")

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-09-09").cast("date")) & (F.col("start_date") >= "2024-01-01"))
    .select("*", F.concat("status", F.lit(" - "), "plan").alias("status_plan"))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .filter(F.col("status").isin(["ACTIVE", "CANCELLED", "EXPIRED"]))
    .select(
        "id",
        "monthly_fee",
        F.lag("monthly_fee").over(w2).alias("prev_monthly_fee"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

remaining_keys = (
    scored
    .select("id")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date") <= "2025-09-01")
        .select("id")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("prev_monthly_fee").between(80, 90))
    .select(
        "*",
        (F.col("prev_monthly_fee") * F.col("row_seq")).alias("prev_monthly_fee_times_row_seq"),
        (F.col("row_seq") + F.col("prev_monthly_fee")).alias("row_seq_plus_prev_monthly_fee"),
    )
)

result = (
    filtered
    .withColumn("prev_monthly_fee_tier", F.when(F.col("prev_monthly_fee") >= 189, "HIGH").otherwise("LOW"))
    .withColumn("row_seq_plus_prev_monthly_fee_tier", F.when(F.col("row_seq_plus_prev_monthly_fee") >= 75, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["prev_monthly_fee_tier", "row_seq_plus_prev_monthly_fee_tier"], ["prev_monthly_fee_tier"], []],
        "prev_monthly_fee_tier",
        "row_seq_plus_prev_monthly_fee_tier",
    )
    .agg(
        F.percentile_approx("row_seq_plus_prev_monthly_fee", [0.25, 0.5, 0.75]).alias("row_seq_plus_prev_monthly_fee_quartiles"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.grouping("prev_monthly_fee_tier").alias("grouping_id"),
    )
    .orderBy(F.col("prev_monthly_fee_tier").desc_nulls_first())
)
