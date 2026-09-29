from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "DELIVERED"]) & (F.col("order_id") == 36))
    .select(
        "id",
        "order_id",
        "status",
        "cost",
        F.dayofmonth("shipped_date").alias("shipped_date_day"),
    )
    .orderBy(F.col("shipped_date_day").desc(), F.col("id").desc())
)
