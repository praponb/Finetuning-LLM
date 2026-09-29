from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "status", "discount")
    .orderBy(F.col("id").desc())
    .limit(20)
)
