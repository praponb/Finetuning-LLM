from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([18, 25, 31, 40]))
    .select("id", "status", "monthly_fee", F.rank().over(w).alias("rnk"))
)
