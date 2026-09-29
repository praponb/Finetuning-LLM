from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("store_id") == 19) & ((F.year("order_date") == 2023) | F.col("customer_id").isin([10, 22, 38])))
    .select(
        "id",
        "order_date",
        "amount",
        "store_id",
        F.when(F.col("status").isin(["COMPLETED", "SHIPPED"]), "Core").otherwise("Other").alias("status_group"),
    )
    .orderBy(F.col("store_id").desc(), "id")
    .limit(50)
)
