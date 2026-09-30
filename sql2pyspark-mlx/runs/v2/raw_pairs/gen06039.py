from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~((F.col("cost") >= 28) & (F.col("delivered_date") < "2025-02-08")))
    .select("id", "order_id", "shipped_date")
    .orderBy(F.col("order_id").desc(), "id")
    .limit(10)
)
