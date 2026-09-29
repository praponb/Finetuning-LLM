from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2025-10-01")
    .select("shipped_date", F.round((F.col("cost") * 1.15), 2).alias("adjusted_cost"))
)
