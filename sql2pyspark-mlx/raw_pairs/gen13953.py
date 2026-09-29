from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(37, 58) & ((F.col("city") == "Bangkok") | F.col("city").isin(["London", "Bangkok"])))
    .agg(F.sum("age").alias("total_age"))
)
