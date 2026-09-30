from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "DHL") | F.col("order_id").isNotNull())
    .select("order_id")
    .orderBy(F.col("order_id").desc())
)
