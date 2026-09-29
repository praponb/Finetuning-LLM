from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date"), "segment").orderBy("age", "id")
w2 = Window.partitionBy("city").orderBy("age", "id")
w3 = Window.partitionBy(F.year("signup_date"), "country").orderBy("credit_limit", "id")
w4 = Window.partitionBy("segment").orderBy("age_bucket", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("channel").orderBy("age", "id")
w7 = Window.partitionBy(F.year("signup_date"), "country").orderBy("age_bucket_2", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("segment").orderBy("last7_sum_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_customers
    .withColumn("age_bucket", F.ntile(5).over(w))
    .withColumn("age_bucket_2", F.ntile(4).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

with_windows = (
    scored
    .withColumn("last7_min_credit_limit", F.min("credit_limit").over(w4))
    .withColumn("last7_max_credit_limit", F.max("credit_limit").over(w4))
    .withColumn("last7_sum_credit_limit", F.sum("credit_limit").over(w4))
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel"))
)

valid = (
    with_latest
    .filter(F.col("is_active") & (F.col("signup_date") >= F.lit("2023-07-24").cast("date")))
    .withColumn("channel_length", F.length("channel"))
)

result = (
    valid
    .filter(F.col("channel").isin(["web", "store"]))
    .select(
        "*",
        F.row_number().over(w6).alias("seq_no"),
        F.avg("age_bucket").over(w7).alias("age_bucket_avg_5_prior"),
        F.sum("last7_max_credit_limit").over(w8).alias("cumulative_last7_max_credit_limit"),
    )
)
