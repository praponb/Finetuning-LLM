from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last())

result = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.max("monthly_fee").over(w).alias("running_max_monthly_fee"),
        F.min("monthly_fee").over(w).alias("running_min_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("running_avg_monthly_fee"),
        F.cume_dist().over(w2).alias("monthly_fee_cume_dist"),
    )
)
