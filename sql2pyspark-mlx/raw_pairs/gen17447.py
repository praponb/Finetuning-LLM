from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee")

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2024-09-04")
    .select("id", "customer_id", "monthly_fee", "plan", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .orderBy(F.col("rnk").desc(), F.col("monthly_fee").desc())
)
