from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .select("id", "plan", "end_date", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "plan", "end_date")
    .orderBy("end_date")
)
