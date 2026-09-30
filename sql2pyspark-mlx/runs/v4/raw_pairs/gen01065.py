from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy(F.col("signup_date").desc_nulls_first(), "id")

filtered = (
    df_customers
    .filter(F.col("name").like("%Olivi%"))
    .select(
        "*",
        F.when(F.col("credit_limit") >= 12800, "High").when(F.col("credit_limit") >= 3000, "Medium").otherwise("Low").alias("credit_limit_tier"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country").alias("session_country"), F.col("c.browser"))
)

latest = (
    with_latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_latest_customers = (
    latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country").alias("session_country_2"))
)

result = (
    with_latest_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"))
    .agg(
        F.bit_and("age").alias("age_bit_and"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("browser") == "Chrome", 1)), F.lit(0)), 2).alias("credit_limit_per_chrome"),
    )
    .orderBy(F.col("credit_limit_per_chrome").desc())
)
