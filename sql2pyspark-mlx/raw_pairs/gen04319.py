from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "cost", "carrier")
    .orderBy(F.col("carrier").desc(), F.col("id").desc())
)
