from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "monthly_fee",
        "end_date",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "status", "monthly_fee", "end_date")
)
