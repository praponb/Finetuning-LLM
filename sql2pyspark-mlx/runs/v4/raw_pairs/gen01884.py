from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-12-01", "2025-08-28"))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
)
