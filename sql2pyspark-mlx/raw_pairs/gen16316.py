from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .select("id", "plan", "monthly_fee", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "plan", "monthly_fee")
    .orderBy("monthly_fee")
)
