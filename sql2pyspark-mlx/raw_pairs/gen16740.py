from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isin([7, 29]))
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
)

result = (
    valid
    .select("order_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("quantity").between(5, 6))
        .select("order_id", "id")
    )
)
