from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "UPS") & F.col("order_id").isNotNull())
    .select("id", "order_id", "carrier", "shipped_date")
    .orderBy("shipped_date")
)
