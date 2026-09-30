from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status", "plan").orderBy("start_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status").orderBy(F.col("start_date_dayofweek").desc(), "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.month("start_date").isin([3, 4, 11]))
    .withColumn("start_date_month", F.month("start_date"))
    .withColumn("start_date_dayofweek", F.dayofweek("start_date"))
)

result = (
    valid
    .filter(F.col("end_date") <= "2024-09-01")
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "start_date_month",
        F.count("start_date_dayofweek").over(w_hist).alias("running_count_start_date_dayofweek"),
        F.max("start_date_dayofweek").over(w_hist).alias("running_max_start_date_dayofweek"),
        F.avg("start_date_dayofweek").over(w_hist).alias("running_avg_start_date_dayofweek"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)
