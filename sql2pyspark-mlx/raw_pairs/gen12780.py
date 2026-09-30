from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.39)
    .select("discount")
    .orderBy(F.col("discount").desc())
)
