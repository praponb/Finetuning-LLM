from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(50, 92) & ((F.col("carrier") == "Flash") | F.col("order_id").isNotNull()))
)
