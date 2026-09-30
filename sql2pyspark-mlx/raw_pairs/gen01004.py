from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 93)
    .select("id", "shipped_date")
    .orderBy(F.col("shipped_date").desc(), "id")
    .limit(50)
)
