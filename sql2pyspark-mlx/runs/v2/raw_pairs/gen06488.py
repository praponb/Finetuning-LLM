from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "IN_TRANSIT") & (F.col("shipped_date") > "2024-08-01"))
    .select("id", "cost", F.round("cost", 0).alias("cost_rounded"))
    .orderBy("id")
)
