from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "shipped_date")
    .orderBy("id", F.col("shipped_date").desc())
)
