from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") != 29)
    .select("sale_date")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isin([4, 35]))
        .select(F.col("delivered_date").alias("sale_date"))
    )
)
