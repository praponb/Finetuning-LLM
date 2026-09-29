from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id").isNull() & (F.col("carrier") == "Kerry")) | (F.col("status") == "LOST"))
    .select(
        "id",
        "carrier",
        F.when(F.col("status") == "DELIVERED", 1).otherwise(0).alias("is_delivered"),
    )
    .orderBy(F.col("id").desc())
)
