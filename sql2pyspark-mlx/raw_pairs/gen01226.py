from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .select("revenue")
    .subtract(df_shipments.filter(F.col("cost").isNotNull()).select("cost"))
)
