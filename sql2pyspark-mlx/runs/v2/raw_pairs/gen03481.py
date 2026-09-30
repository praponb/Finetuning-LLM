from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "enterprise", "standard"]))
    .select("id", "status", "monthly_fee", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .orderBy(F.col("status").desc())
)
