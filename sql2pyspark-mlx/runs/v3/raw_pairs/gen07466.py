from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .select("id", "plan", "monthly_fee", "status", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("id").desc())
)
