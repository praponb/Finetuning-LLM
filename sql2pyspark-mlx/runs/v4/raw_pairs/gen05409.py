from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment")
w3 = Window.partitionBy("city", "country").orderBy("signup_date", "id")
w4 = Window.partitionBy("account_status", "country").orderBy(F.col("age").desc(), "id")
w5 = Window.orderBy("age", "id")
w6 = Window.partitionBy("segment").orderBy(F.col("age_bucket_2").desc())
w_part = Window.partitionBy("city").orderBy(F.col("corr_credit_limit_age").desc(), "id")

filtered = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.substring("segment", 1, 2).alias("segment_prefix"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("account_status"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") < F.col("s.signup_date")), "left_anti")
)

enriched = (
    kept
    .select(
        "*",
        F.corr("credit_limit", "age").over(w2).alias("corr_credit_limit_age"),
        F.lag("credit_limit", 1).over(w3).alias("prev_credit_limit"),
        F.ntile(4).over(w4).alias("age_bucket"),
        F.ntile(4).over(w5).alias("age_bucket_2"),
    )
)

enriched_customers = (
    enriched
    .withColumn("age_bucket_2_rank", F.rank().over(w6))
    .withColumn("event_seq", F.row_number().over(w_part))
)

result = (
    enriched_customers
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "credit_limit",
        "age",
        "segment_prefix",
        "corr_credit_limit_age",
        F.when(F.col("segment").rlike("^(Consumer|SMB)$"), "CORE").otherwise("PRIORITY").alias("segment_zone"),
    )
    .orderBy(F.col("age").asc_nulls_first())
)
