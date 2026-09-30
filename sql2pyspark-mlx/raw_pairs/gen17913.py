from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").isNotNull())
    .select("cost")
    .union(
        df_order_items
        .filter(F.col("unit_price").isNotNull())
        .select("unit_price")
    )
    .distinct()
    .orderBy(F.col("cost").desc())
)
