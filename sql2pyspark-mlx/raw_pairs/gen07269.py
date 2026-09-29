from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy(F.col("age").asc_nulls_first(), "id")
w2 = Window.partitionBy("segment", "city").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

ranked = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.to_date(F.col("x.started_at")) >= "2024-06-01"), "left_anti")
)

cleaned = (
    kept
    .filter(F.col("is_active"))
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
        "rn",
        F.greatest("credit_limit", "age").alias("greatest_credit_limit_age"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "country",
        "segment",
        "is_active",
        "credit_limit",
        "rn",
        F.avg("greatest_credit_limit_age").over(w2).alias("running_avg_greatest_credit_limit_age"),
        F.max("greatest_credit_limit_age").over(w2).alias("running_max_greatest_credit_limit_age"),
        F.sum("greatest_credit_limit_age").over(w2).alias("running_sum_greatest_credit_limit_age"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.balance"))
)

result = (
    with_last
    .select(
        "id",
        "segment",
        "credit_limit",
        "running_max_greatest_credit_limit_age",
        "running_sum_greatest_credit_limit_age",
        "balance",
        F.when(F.col("segment").rlike("^(SMB|Corporate)$"), "GROUP_A").when(F.col("segment").rlike("^(Consumer)$"), "EXTENDED").otherwise("CORE").alias("segment_zone"),
    )
    .orderBy(F.col("balance").desc_nulls_first(), "id")
)
