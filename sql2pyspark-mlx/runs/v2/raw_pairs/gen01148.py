from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date")
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "end_date",
        "customer_id",
        "monthly_fee",
        F.rank().over(w).alias("end_date_rank"),
        F.rank().over(w2).alias("rnk"),
    )
)
