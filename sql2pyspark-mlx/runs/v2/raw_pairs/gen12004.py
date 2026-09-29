from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .select("id", "plan", "end_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "plan", "end_date")
    .orderBy("plan")
)
