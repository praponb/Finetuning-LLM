from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "standard"]))
    .select("id", "plan", "start_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .orderBy("plan")
)
