from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select("id", "status", "monthly_fee", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "status", "monthly_fee")
)
