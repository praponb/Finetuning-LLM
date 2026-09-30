from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").isNotNull())
    .select("sale_date")
    .union(
        df_shipments
        .filter(F.col("shipped_date").isNotNull())
        .select("shipped_date")
    )
    .orderBy(F.col("sale_date").desc())
)
