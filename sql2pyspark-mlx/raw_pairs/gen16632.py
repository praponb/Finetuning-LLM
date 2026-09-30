from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status").orderBy("end_date", "id")
w2 = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "ACTIVE", "PAUSED"]))
    .select(
        "*",
        F.lead("monthly_fee").over(w_part).alias("next_monthly_fee"),
        F.row_number().over(w_part).alias("row_seq"),
        F.percent_rank().over(w2).alias("monthly_fee_percent_rank"),
    )
)
