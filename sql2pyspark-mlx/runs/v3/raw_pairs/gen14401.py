from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .union(
        df_orders
        .filter(F.col("customer_id").isin([7, 29, 38]))
        .select("order_date")
    )
    .orderBy(F.col("launch_date").desc())
)
