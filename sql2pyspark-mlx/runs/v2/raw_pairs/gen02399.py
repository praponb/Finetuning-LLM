from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w2 = Window.orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 200)
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "end_date",
        F.ntile(3).over(w).alias("bucket"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy("id")
)
