from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([4, 12, 14]))
    .select("id", "monthly_fee", F.ntile(2).over(w).alias("bucket"))
)
