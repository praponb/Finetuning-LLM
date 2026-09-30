from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date")
w2 = Window.partitionBy("plan")

result = (
    df_subscriptions
    .filter(F.col("end_date") <= "2023-08-01")
    .select(
        "id",
        "start_date",
        "plan",
        "monthly_fee",
        F.percent_rank().over(w).alias("start_date_pct_rank"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 3).alias("monthly_fee_share"),
    )
)
