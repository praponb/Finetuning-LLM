from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select("id", "plan", "monthly_fee", F.row_number().over(w).alias("rn"))
)
