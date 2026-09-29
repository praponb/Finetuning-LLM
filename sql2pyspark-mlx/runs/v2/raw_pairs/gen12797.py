from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .select("id", "status", "monthly_fee", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
)
