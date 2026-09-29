from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([1, 8, 21, 39]))
    .select("id", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("customer_id") == 34)
        .select("id", "customer_id")
    )
    .orderBy("id")
)
