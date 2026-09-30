from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("signup_date", "id")
w3 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w4 = Window.partitionBy("city")

enriched = (
    df_customers
    .filter(F.col("signup_date") <= "2025-02-18")
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w))
    .withColumn("prev_age", F.lag("age", 1).over(w2))
)

kept = (
    enriched
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.signup_date")), "left_anti")
)

base = (
    kept
    .filter(F.col("prev_age").between(45, 49))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "prev_age",
        F.when(F.col("city") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
        F.regexp_replace("name", " ", "").alias("name_slug"),
    )
)

base_customers = (
    base
    .filter(F.col("age").isNull() & (F.col("signup_date") >= F.lit("2023-09-02").cast("date")))
    .withColumn("email_title", F.initcap(F.trim("email")))
    .withColumn("city_title", F.initcap(F.trim("city")))
    .withColumn("resolved_city", F.coalesce("city", F.lit("UNKNOWN")))
)

eligible = (
    base_customers
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.end_date") <= F.col("s.signup_date")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "city",
        "segment",
        "age",
        "prev_age",
        "name_slug",
        "city_title",
        "resolved_city",
        F.ntile(10).over(w3).alias("credit_limit_bucket"),
        F.count("prev_age").over(w4).alias("city_count_prev_age"),
    )
)
