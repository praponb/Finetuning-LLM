from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "end_date",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
    )
    .orderBy(F.col("status").desc())
)
