from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.month("signup_date"), "device").orderBy("age", "id")
w3 = Window.partitionBy(F.month("signup_date"), "device").orderBy("signup_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device").orderBy("age", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("country").orderBy("age", "id")
w7 = Window.partitionBy("country")
w8 = Window.partitionBy("country").orderBy(F.col("jp_total_age_avg_6").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.name", "s.country", "s.signup_date", "s.age", F.col("c.device"))
)

scored = (
    latest_match
    .withColumn("prev_age", F.lag("age", 1, 0).over(w2))
    .withColumn("age_avg_6", F.avg("age").over(w3))
    .withColumn("running_age", F.sum("age").over(w4))
    .withColumn("age_avg_5", F.avg("age").over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

kept = (
    scored
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") >= F.col("s.signup_date")), "left_anti")
)

remaining_keys = (
    kept
    .select("id")
    .subtract(
        df_customers
        .filter(F.col("credit_limit").isNull())
        .select("id")
    )
)

kept_rows = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter((F.col("signup_date") >= F.lit("2023-06-16").cast("date")) & F.col("device").isNotNull())
    .withColumn("signup_date_plus_2d", F.date_add("signup_date", 2))
    .withColumn("age_avg_6_level", F.when(F.col("age_avg_6") >= 62, "High").when(F.col("age_avg_6") >= 30, "Medium").otherwise("Low"))
)

metrics = (
    valid
    .select(
        "id",
        "country",
        "age_avg_6",
        "running_age",
        "signup_date_plus_2d",
        "age_avg_6_level",
        F.round((F.col("age_avg_5") / F.sum("age_avg_5").over(w7)), 4).alias("age_avg_5_share"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.status").isin(["OPEN", "FROZEN"])), "left_anti")
)

totals = (
    eligible
    .groupBy(
        "country",
        F.date_trunc("QUARTER", "signup_date_plus_2d").alias("quarter_start"),
    )
    .agg(
        F.max("running_age").alias("max_running_age"),
        F.sum(F.when(F.col("country") == "JP", F.col("age_avg_6"))).alias("jp_total_age_avg_6"),
        F.percentile_approx("running_age", [0.25, 0.5, 0.75]).alias("running_age_quartiles"),
    )
    .withColumn("jp_total_age_avg_6_dense_rank", F.dense_rank().over(w8))
)

result = (
    totals
    .select(
        "country",
        "quarter_start",
        "jp_total_age_avg_6",
        "running_age_quartiles",
        "jp_total_age_avg_6_dense_rank",
    )
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
