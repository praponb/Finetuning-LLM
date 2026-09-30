from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "plan", "start_date", F.ntile(2).over(w).alias("start_date_ntile"))
)
