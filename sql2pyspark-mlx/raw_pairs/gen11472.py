from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("is_active", "id")
w2 = Window.partitionBy("status", "customer_id").orderBy(F.col("is_active").desc(), "id")
w3 = Window.orderBy("end_date", "id").rowsBetween(-5, -1)

valid = (
    df_subscriptions
    .filter(F.col("end_date") < "2023-08-01")
    .withColumn("is_active", F.when(F.col("status") == "ACTIVE", 1).otherwise(0))
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        "plan",
        "status",
        F.ntile(3).over(w).alias("is_active_bucket"),
        F.ntile(3).over(w2).alias("is_active_bucket_2"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_5_prior"),
    )
)
