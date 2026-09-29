from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("channel").alias("label"))
    .unionByName(
        df_suppliers
        .filter(F.col("country") != "DE")
        .select(F.col("city").alias("label"))
    )
)
