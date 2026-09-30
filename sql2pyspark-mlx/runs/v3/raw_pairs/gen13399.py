from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-07-01", "2024-09-13"))
    .select("order_id")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") >= "2023-07-18")
        .select("order_id")
    )
    .orderBy(F.col("order_id").desc())
)
