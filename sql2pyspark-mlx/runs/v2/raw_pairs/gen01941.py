from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("start_date")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "start_date",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 3).alias("monthly_fee_share"),
        F.dense_rank().over(w2).alias("start_date_dense_rank"),
    )
    .orderBy(F.col("monthly_fee").desc())
)
