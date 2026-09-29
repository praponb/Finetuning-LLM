from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy(F.date_trunc("month", "signup_date"), "city").orderBy("credit_limit", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("segment").orderBy(F.col("start_date").desc(), "id")
w5 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.year("start_date"), "segment")

filtered = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-03-10").cast("date"))
    .select(
        "*",
        F.trim("name").alias("clean_name"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_filled"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "clean_name",
        "segment_filled",
        F.lead("credit_limit", 1).over(w).alias("next_credit_limit"),
        F.ntile(5).over(w2).alias("credit_limit_bucket"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.segment",
        "s.is_active",
        "s.credit_limit",
        "s.clean_name",
        "s.segment_filled",
        "s.credit_limit_bucket",
        F.col("c.start_date"),
    )
)

metrics = (
    with_latest
    .filter(~F.col("is_active"))
    .select(
        "id",
        "email",
        "segment",
        "credit_limit",
        "clean_name",
        "segment_filled",
        "credit_limit_bucket",
        F.row_number().over(w4).alias("seq_no"),
        F.last("credit_limit", ignorenulls=True).over(w5).alias("last_known_credit_limit"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w6)), 4).alias("credit_limit_share"),
    )
)

result = (
    metrics
    .select(
        "id",
        "email",
        "segment_filled",
        "credit_limit_bucket",
        "seq_no",
        "last_known_credit_limit",
        "credit_limit_share",
    )
)
