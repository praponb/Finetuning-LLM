from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment").orderBy("age", "id")
w6 = Window.partitionBy("city").orderBy(F.col("prev_age").desc(), "id")
w_recent = Window.partitionBy("country")
w_recent_2 = Window.partitionBy("segment", "country").orderBy(F.col("prev_age").desc(), "id")
w9 = Window.partitionBy("city", "country")
w10 = Window.partitionBy("city")

scored = (
    df_customers
    .withColumn("prev_age", F.lag("age", 1, 0).over(w))
    .withColumn("running_credit_limit", F.sum("credit_limit").over(w2))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w3))
    .withColumn("first_known_city_2", F.first("city", ignorenulls=True).over(w4))
    .withColumn("age_bucket", F.ntile(10).over(w5))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.signup_date")), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("country_min_credit_limit", F.min("credit_limit").over(w_recent))
    .withColumn("seq_no_2", F.row_number().over(w_recent_2))
    .withColumn("city_stddev_samp_age", F.stddev_samp("age").over(w9))
    .withColumn("city_max_prev_age", F.max("prev_age").over(w10))
)

eligible = (
    metrics
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
    )
    .orderBy(F.col("segment").desc(), "id")
    .limit(10)
)
