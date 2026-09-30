from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .intersect(
        df_transactions
        .filter(F.col("amount").between(1500, 2400))
        .select(F.col("amount").alias("cost"))
    )
    .orderBy("cost")
)
