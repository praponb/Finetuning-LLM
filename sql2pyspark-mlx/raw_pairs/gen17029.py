from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").isNotNull())
    .select("delivered_date")
    .union(
        df_products
        .filter(F.col("launch_date").isNotNull())
        .select("launch_date")
    )
    .distinct()
)
