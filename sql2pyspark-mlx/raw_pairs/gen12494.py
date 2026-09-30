from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date_month_end").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city", "segment")
w5 = Window.orderBy("credit_limit_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.month("signup_date"), "city").orderBy("credit_limit_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city", "status").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status", "city")
w9 = Window.orderBy("credit_limit", "id").rowsBetween(-6, -1)

cleaned = (
    df_customers
    .filter(F.col("is_active"))
    .withColumn("signup_date_month_end", F.last_day("signup_date"))
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.amount"))
)

with_windows = (
    with_latest
    .filter(F.col("city") != "Bangkok")
    .select(
        "id",
        "email",
        "city",
        "signup_date",
        "credit_limit",
        "age",
        "latest_rank",
        "status",
        "amount",
        F.sum("credit_limit").over(w_part).alias("running_sum_credit_limit"),
        F.avg("credit_limit").over(w_part).alias("running_avg_credit_limit"),
        F.min("credit_limit").over(w_part).alias("running_min_credit_limit"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4).alias("credit_limit_share"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.sum("running_sum_credit_limit").over(w5).alias("running_running_sum_credit_limit"),
        F.sum("age").over(w6).alias("cumulative_age"),
        F.last("credit_limit_share", ignorenulls=True).over(w7).alias("last_known_credit_limit_share"),
        F.corr("credit_limit", "age").over(w8).alias("corr_credit_limit_age"),
        F.avg("age").over(w9).alias("age_avg_6_prior"),
    )
)

result = (
    scored
    .select(
        "id",
        "email",
        "city",
        "credit_limit",
        "latest_rank",
        "status",
        "running_sum_credit_limit",
        "running_avg_credit_limit",
        "credit_limit_share",
        F.when(F.col("city").rlike("^(Chiang Mai|Tokyo)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|New York)$"), "CORE").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("id").desc())
)
