from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "plan",
        F.rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .select("id", "customer_id", "monthly_fee", "plan")
)
