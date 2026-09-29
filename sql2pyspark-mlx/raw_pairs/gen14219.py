from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .select("shipping_fee")
    .union(
        df_shipments
        .filter(F.col("shipped_date") <= "2025-08-10")
        .select("cost")
    )
    .distinct()
    .orderBy(F.col("shipping_fee").desc())
)
