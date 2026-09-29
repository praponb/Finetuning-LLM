from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").isNotNull())
    .select(F.lit("store_sale").alias("record_type"), "sale_date")
    .union(
        df_shipments
        .filter(F.col("shipped_date").isNotNull())
        .select(F.lit("shipment").alias("record_type"), "shipped_date")
    )
)
