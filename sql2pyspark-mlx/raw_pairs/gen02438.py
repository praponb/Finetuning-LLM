from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").isNotNull())
    .select("order_date")
    .union(
        df_store_sales
        .filter(F.col("sale_date").isNotNull())
        .select("sale_date")
    )
    .distinct()
)
