from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee", "id")
w2 = Window.partitionBy("plan").orderBy("weeks_end_date_to_start_date", "id")
w3 = Window.partitionBy("status", "plan").orderBy("weeks_end_date_to_start_date", "id")

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-10-23").cast("date")) & F.month("start_date").isin([2, 7, 11]))
    .withColumn("weeks_end_date_to_start_date", (F.datediff("start_date", "end_date") / 7))
    .withColumn("monthly_fee_adjusted", F.round((F.col("monthly_fee") * 1.1), 2))
)

result = (
    filtered
    .select(
        "*",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
        F.lag("monthly_fee", 1).over(w2).alias("prev_monthly_fee"),
        F.ntile(5).over(w3).alias("weeks_end_date_to_start_date_bucket"),
    )
)
