from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("tag"))

joined = (
    lines
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.id", "e.customer_id", F.col("d.signup_date"))
)

valid = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.last_day("signup_date").alias("signup_date_month_end"))
)

joined_carts = (
    valid
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.customer_id", "e.signup_date_month_end", F.col("p.credit_limit"))
)

joined_carts_2 = (
    joined_carts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.signup_date"), F.col("c.segment"))
)

result = (
    joined_carts_2
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .rollup("activity_month")
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.grouping("activity_month").alias("grouping_id"),
    )
    .orderBy(F.col("activity_month").asc_nulls_first())
)
