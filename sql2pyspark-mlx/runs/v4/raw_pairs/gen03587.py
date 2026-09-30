from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .select("id", "status", "start_date", "plan", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .orderBy(F.col("status").desc(), "plan")
)
