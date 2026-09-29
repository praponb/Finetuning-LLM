from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .withColumn("avg_credit_limit_by_country", F.avg("credit_limit").over(w))
    .filter(F.col("credit_limit") > F.col("avg_credit_limit_by_country"))
    .select("id", "country", "credit_limit", "is_active")
)
