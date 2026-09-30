from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") < 274)
    .select("launch_date")
    .subtract(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select(F.col("delivered_date").alias("launch_date"))
    )
)
