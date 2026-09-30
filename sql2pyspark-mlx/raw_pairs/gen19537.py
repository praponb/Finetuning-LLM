from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date")

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .select("id", "customer_id", "start_date", F.rank().over(w).alias("rnk"))
)
