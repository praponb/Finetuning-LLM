from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select("id", "shipped_date", "status")
    .orderBy(F.col("id").desc())
)
