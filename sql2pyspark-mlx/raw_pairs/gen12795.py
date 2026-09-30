from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "signup_date"), "city")
w2 = Window.orderBy(F.col("credit_limit").desc(), "id")

valid = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-07-05").cast("date"))
    .withColumn("email_prefix", F.substring("email", 1, 3))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("UNKNOWN")))
)

result = (
    valid
    .filter(F.col("city").isin(["Singapore", "London", "New York"]))
    .select(
        "*",
        F.avg("credit_limit").over(w).alias("signup_date_period_avg_credit_limit"),
        F.ntile(4).over(w2).alias("credit_limit_bucket"),
    )
)
