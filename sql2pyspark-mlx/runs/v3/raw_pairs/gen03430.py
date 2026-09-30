from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .unionByName(
        df_shipments
        .filter(F.year("shipped_date") == 2025)
        .select(F.col("cost").alias("unit_price"))
    )
    .distinct()
    .orderBy(F.col("unit_price").desc())
)
