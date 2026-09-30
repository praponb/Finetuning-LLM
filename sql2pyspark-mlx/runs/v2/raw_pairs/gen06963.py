from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "DHL") & (F.col("cost") > 96))
    .select("id", "cost", "delivered_date")
    .orderBy(F.col("cost").desc())
)
