from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("monthly_fee").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy("start_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id")
w5 = Window.orderBy(F.col("monthly_fee").desc(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-08-20").cast("date"))
    .select("*", F.dayofweek("start_date").alias("start_date_dayofweek"))
)

result = (
    filtered
    .select(
        "id",
        "plan",
        "end_date",
        "status",
        F.dense_rank().over(w).alias("monthly_fee_dense_rank"),
        F.first("start_date_dayofweek", ignorenulls=True).over(w2).alias("first_known_start_date_dayofweek"),
        F.last("monthly_fee", ignorenulls=True).over(w3).alias("last_known_monthly_fee"),
        F.stddev_samp("monthly_fee").over(w4).alias("customer_id_stddev_samp_monthly_fee"),
        F.ntile(3).over(w5).alias("monthly_fee_bucket"),
    )
)
