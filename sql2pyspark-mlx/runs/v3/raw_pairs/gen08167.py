from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee", F.lit("order").alias("source"))
    .unionByName(
        df_products
        .filter(F.col("brand").isin(["Umbrella", "Stark"]))
        .select(F.col("cost").alias("shipping_fee"), F.lit("product").alias("source"))
    )
    .orderBy("shipping_fee")
)
