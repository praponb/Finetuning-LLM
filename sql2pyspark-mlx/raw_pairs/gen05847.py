from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-08-23").cast("date")) & F.col("city").isNotNull())
    .withColumn("email_user", F.split(F.lower("email"), "@").getItem(0))
    .withColumn("segment_clean", F.coalesce("segment", F.lit("OTHER")))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.start_date") <= F.col("s.signup_date")), "left_semi")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.signup_date")), "left_anti")
)

enriched = (
    kept
    .filter(F.col("segment") != "Corporate")
    .select(
        "*",
        F.sum("age").over(w).alias("running_sum_age"),
        F.count("age").over(w).alias("running_count_age"),
        F.min("age").over(w).alias("running_min_age"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.status") == "PAUSED"), "left_semi")
)

by_type = (
    eligible
    .groupBy("country", "city")
    .pivot("segment", ["Consumer", "Corporate"])
    .agg(F.avg("age"))
)

result = by_type.select("country", "city", "Consumer", "Corporate")
