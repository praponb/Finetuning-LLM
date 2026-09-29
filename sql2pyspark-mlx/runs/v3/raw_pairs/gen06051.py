from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "start_date",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "customer_id", "monthly_fee", "start_date")
)
