from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(F.col("order_id").isin([8, 29]) | F.col("delivered_date").between("2025-04-01", "2025-06-01")))
    .select("cost", "delivered_date", "order_id")
    .orderBy(F.col("order_id").desc())
)
