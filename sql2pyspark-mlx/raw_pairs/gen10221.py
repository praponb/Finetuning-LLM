from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("segment", ["Consumer", "Corporate"])
    .agg(F.avg("credit_limit").alias("value_sum"))
)
