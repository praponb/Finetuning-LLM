from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("credit_limit").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy("credit_limit_scaled")
w4 = Window.partitionBy("plan").orderBy("credit_limit_scaled", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("plan").orderBy(F.col("credit_limit_scaled").desc(), "id")
w6 = Window.partitionBy("plan").orderBy(F.col("credit_limit").desc_nulls_first())
w7 = Window.partitionBy("plan").orderBy(F.col("credit_limit_cume_dist").desc_nulls_first(), "id")
w8 = Window.partitionBy("credit_limit_scaled_dense_rank_tier").orderBy(F.col("avg_credit_limit_percent_rank").desc())

filtered = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-03-15").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "signup_date",
        "credit_limit",
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.amount") >= 1420), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("signup_date") > F.lit("2023-05-24").cast("date"))
    .withColumn("clean_email", F.trim("email"))
    .withColumn("signup_date_label", F.date_format("signup_date", "MMM yyyy"))
)

with_last = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.credit_limit",
        "s.city_title",
        "s.clean_email",
        "s.signup_date_label",
        F.col("c.plan"),
    )
)

base = (
    with_last
    .filter(~F.col("plan").eqNullSafe(F.lit("basic")))
    .select(
        "*",
        F.round((F.col("credit_limit") * 1.07), 2).alias("credit_limit_scaled"),
    )
)

with_windows = (
    base
    .filter(F.col("credit_limit_scaled").isNull())
    .select(
        "id",
        "name",
        "signup_date_label",
        "plan",
        F.cume_dist().over(w2).alias("credit_limit_cume_dist"),
        F.dense_rank().over(w3).alias("credit_limit_scaled_dense_rank"),
        F.avg("credit_limit").over(w4).alias("credit_limit_avg_3_prior"),
        F.ntile(4).over(w5).alias("credit_limit_scaled_bucket"),
        F.percent_rank().over(w6).alias("credit_limit_percent_rank"),
    )
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("credit_limit_scaled_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("credit_limit_scaled_dense_rank_tier"),
        F.when(F.col("credit_limit_avg_3_prior") >= 12449, "PREMIUM").otherwise("STANDARD").alias("credit_limit_avg_3_prior_tier"),
        "plan",
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.round(F.avg("credit_limit_percent_rank"), 2).alias("avg_credit_limit_percent_rank"),
        F.round(F.avg("credit_limit_cume_dist"), 2).alias("avg_credit_limit_cume_dist"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
    )
    .withColumn("avg_credit_limit_percent_rank_dense_rank", F.dense_rank().over(w8))
    .orderBy(F.col("credit_limit_avg_3_prior_tier").asc_nulls_last())
)
