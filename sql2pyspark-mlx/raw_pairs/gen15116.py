from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id")
w3 = Window.partitionBy("segment").orderBy("age", "id")
w4 = Window.partitionBy(F.year("signup_date")).orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment").orderBy("age_share", "id")
w_grp = Window.partitionBy("segment").orderBy("age_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("segment")

scored = (
    df_customers
    .filter(F.col("age").isNull())
    .select(
        "id",
        "email",
        "segment",
        "signup_date",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.lag("age", 1).over(w2).alias("prev_age"),
        F.ntile(10).over(w3).alias("age_bucket"),
        F.last("credit_limit", ignorenulls=True).over(w4).alias("last_known_credit_limit"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.lag("age_bucket").over(w5).alias("prev_age_bucket"),
        F.lag("last_known_credit_limit").over(w5).alias("prev_last_known_credit_limit"),
        F.lead("last_known_credit_limit").over(w5).alias("next_last_known_credit_limit"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

cleaned = (
    enriched
    .filter((F.col("signup_date") > F.lit("2023-03-05").cast("date")) & (F.col("prev_age") != 61))
    .select(
        "*",
        (F.col("prev_age") * F.col("seq_no")).alias("prev_age_times_seq_no"),
        F.col("age_bucket").cast("string").alias("age_bucket_num"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.signup_date")), "left_semi")
)

metrics = (
    not_flagged
    .withColumn("running_prev_last_known_credit_limit", F.sum("prev_last_known_credit_limit").over(w_grp))
    .withColumn("segment_max_prev_age_bucket", F.max("prev_age_bucket").over(w_recent))
)

result = (
    metrics
    .groupBy("segment")
    .agg(
        F.max(F.when(F.col("segment") == "SMB", F.col("prev_age_times_seq_no"))).alias("smb_max_prev_age_times_seq_no"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_prev_last_known_credit_limit", 0.95).alias("running_prev_last_known_credit_limit_p95"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
