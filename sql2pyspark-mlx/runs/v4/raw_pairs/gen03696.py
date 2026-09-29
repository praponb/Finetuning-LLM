from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("plan", "status").orderBy(F.col("monthly_fee").desc_nulls_first())

metrics = (
    df_subscriptions
    .select(
        "*",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "next_monthly_fee",
        "monthly_fee_dense_rank",
    )
    .orderBy(F.col("status").desc())
)
