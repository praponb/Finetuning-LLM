from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .select("id", "status", "monthly_fee", "plan", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .orderBy(F.col("id").desc(), "status")
)
