from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "COMPLETED")
    .select("id", "status")
)
