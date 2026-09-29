from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "DELIVERED", "RETURNED"]))
    .select("shipped_date")
    .intersect(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select(F.col("launch_date").alias("shipped_date"))
    )
)
