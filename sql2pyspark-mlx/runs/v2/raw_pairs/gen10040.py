from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("shipped_date") >= 2025) & F.col("order_id").isNull() & F.month("shipped_date").isin([6, 8, 9]))
    .select("id", "order_id", "delivered_date", "cost")
    .orderBy("id")
)
