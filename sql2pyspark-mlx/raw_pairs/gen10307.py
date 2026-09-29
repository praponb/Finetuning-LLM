from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_credit_limit").desc())

result = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .groupBy("city")
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("total_credit_limit").desc())
)
