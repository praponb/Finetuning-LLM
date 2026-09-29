from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount").desc_nulls_first(), "id")

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-05-14").cast("date")) & F.col("store_id").isin([4, 6, 22, 26]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "shipping_fee",
        F.greatest("discount", "amount").alias("greatest_discount_amount"),
    )
)

result = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_id", "order_date", "amount")
)
