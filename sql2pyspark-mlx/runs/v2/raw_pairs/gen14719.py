from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 38) | F.col("customer_id").isin([16, 20, 22, 38]))
    .select(
        "id",
        "discount",
        F.round((F.col("amount") * 0.85), 2).alias("adjusted_amount"),
    )
)
