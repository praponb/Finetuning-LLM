from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "ship_to",
        "attributes",
        F.col("attributes")["gift"].alias("gift"),
        F.col("attributes")["device"].alias("device"),
        F.col("ship_to.city").alias("ship_city"),
    )
)
