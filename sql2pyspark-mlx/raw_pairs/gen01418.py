from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_cohort")

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["CANCELLED", "ACTIVE"]))
    .groupBy(
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
    )
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_monthly_fee", F.sum("total_monthly_fee").over(w))
)
