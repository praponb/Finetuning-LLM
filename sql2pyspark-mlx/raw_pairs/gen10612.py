from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.date_trunc("WEEK", "signup_date"), "segment")
w3 = Window.partitionBy("city").orderBy("age_per_balance", "id")
w4 = Window.partitionBy("city").orderBy("age_per_balance")
w5 = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_first(), "id")
w6 = Window.partitionBy("city").orderBy("signup_date", "id")
w7 = Window.orderBy("age_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city").orderBy("age_per_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("segment").orderBy("balance", "id")

cleaned = (
    df_customers
    .filter((~F.col("city").eqNullSafe(F.lit("Bangkok"))) & (F.col("credit_limit") <= 14200))
    .withColumn("name_upper", F.upper("name"))
    .withColumn("email_name", F.concat("email", F.lit(" - "), "name"))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.discount").isNull()), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.city",
        "s.segment",
        "s.signup_date",
        "s.age",
        "s.name_upper",
        F.col("c.balance"),
    )
)

cleaned_customers = (
    latest_match
    .filter((F.col("signup_date") >= F.lit("2023-03-13").cast("date")) & (~F.col("city").eqNullSafe(F.lit("Tokyo"))))
    .withColumn("email_normalized", F.upper(F.trim("email")))
    .withColumn("age_per_balance", (F.col("age") / F.nullif("balance", F.lit(0))))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("NONE")))
)

with_windows = (
    cleaned_customers
    .filter(F.col("balance") >= 28000)
    .select(
        "*",
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.ntile(4).over(w3).alias("age_per_balance_bucket"),
        F.dense_rank().over(w4).alias("age_per_balance_dense_rank"),
    )
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("last_known_segment", F.last("segment", ignorenulls=True).over(w7))
    .withColumn("cumulative_age", F.sum("age").over(w8))
    .withColumn("age_per_balance_bucket_2", F.ntile(4).over(w3))
    .withColumn("prev_age_per_balance", F.lag("age_per_balance", 1).over(w9))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("age_per_balance") >= 65, "HIGH").otherwise("LOW").alias("age_per_balance_tier"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.percentile_approx("age_per_balance_bucket_2", 0.9).alias("age_per_balance_bucket_2_p90"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .filter(F.col("age_per_balance_bucket_2_p90") < 1000000)
    .orderBy(F.col("age_per_balance_tier").desc())
)
