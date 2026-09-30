from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "device").orderBy(F.col("started_at").asc_nulls_last(), "id")
w2 = Window.partitionBy("customer_country", "browser").orderBy("age", "id").rowsBetween(-3, Window.currentRow)

latest = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    latest
    .filter((F.col("started_at") >= F.lit("2023-05-13").cast("date")) & (F.col("country") == "SG"))
    .withColumn("browser_group", F.when(F.col("browser").isin(["Chrome", "Edge"]), "Core").otherwise("Other"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("OTHER")))
)

with_dims = (
    base
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.device",
        "a.browser",
        "a.country",
        F.col("b.country").alias("customer_country"),
        F.col("b.credit_limit"),
        F.col("b.age"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("src.*", F.col("c.name"), F.col("c.city"), F.col("c.signup_date"))
)

metrics = (
    joined
    .select(
        "*",
        F.min("credit_limit").over(w2).alias("last4_min_credit_limit"),
        F.sum("credit_limit").over(w2).alias("last4_sum_credit_limit"),
        F.avg("credit_limit").over(w2).alias("last4_avg_credit_limit"),
    )
)

totals = (
    metrics
    .groupBy("country", F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("device") == "mobile", F.col("age"))).alias("mobile_total_age"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
)

result = (
    totals
    .select("country", "year_start")
    .orderBy(F.col("year_start").desc_nulls_last())
)
