from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() | F.month("delivered_date").isin([5, 9, 10]))
    .select("id", "cost", "carrier", "status")
    .orderBy(F.col("id").desc())
    .limit(10)
)
