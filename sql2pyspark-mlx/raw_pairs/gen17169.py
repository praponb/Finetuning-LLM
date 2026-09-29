from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("row_num").desc(), "id")
w3 = Window.partitionBy("country", "segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("segment")
w7 = Window.partitionBy("country").orderBy(F.col("signup_date").desc())
w8 = Window.partitionBy("segment").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("country").orderBy("cumulative_credit_limit", "id")
w10 = Window.partitionBy("segment", "country").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

eligible = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") < F.col("s.signup_date")), "left_anti")
)

latest = (
    eligible
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w3))
    .withColumn("last_known_segment", F.last("segment", ignorenulls=True).over(w4))
    .withColumn("cumulative_credit_limit_2", F.sum("credit_limit").over(w5))
    .withColumn("segment_min_age", F.min("age").over(w6))
)

latest_customers = (
    scored
    .withColumn("latest_rank", F.rank().over(w7))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest_customers
    .select(
        "id",
        "name",
        "email",
        "city",
        "signup_date",
        "is_active",
        "age",
        "row_num",
        "seq_no",
        "cumulative_credit_limit",
        "latest_rank",
        F.sum("segment_min_age").over(w8).alias("running_segment_min_age"),
        F.row_number().over(w_part).alias("event_seq"),
        F.sum("seq_no").over(w10).alias("cumulative_seq_no"),
    )
)

summary = (
    metrics
    .groupBy("seq_no", F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.avg(F.when(F.col("city") == "Singapore", F.col("seq_no"))).alias("singapore_avg_seq_no"),
        F.min("running_segment_min_age").alias("min_running_segment_min_age"),
    )
)

result = (
    summary
    .select("seq_no", "year_start", "singapore_avg_seq_no")
    .orderBy(F.col("seq_no").desc())
)
