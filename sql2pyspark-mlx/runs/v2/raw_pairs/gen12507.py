from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date") >= "2023-03-01")
        .select("status")
    )
)
