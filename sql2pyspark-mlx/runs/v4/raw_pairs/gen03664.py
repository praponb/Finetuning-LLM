from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("signup_date", "id")
w3 = Window.partitionBy("city", "country")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .withColumn("signup_date_month_end", F.last_day("signup_date"))
)

enriched = (
    filtered
    .select(
        "*",
        F.corr("age", "credit_limit").over(w).alias("corr_age_credit_limit"),
        F.lead("credit_limit", 1).over(w2).alias("next_credit_limit"),
        F.max("age").over(w3).alias("city_max_age"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id").isin([14, 26])), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.device"), F.col("c.browser").alias("session_browser"))
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("corr_age_credit_limit") >= 0.58, "LARGE").otherwise("SMALL").alias("corr_age_credit_limit_tier"),
        F.when(F.col("age") >= 56, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
        "segment",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("session_browser") == "Chrome", 1)).alias("chrome_count"),
    )
    .filter(F.col("distinct_ids") >= 1)
)
