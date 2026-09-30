from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "monthly_fee",
        "end_date",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 3).alias("monthly_fee_share"),
        F.dense_rank().over(w2).alias("end_date_dense_rank"),
    )
)
