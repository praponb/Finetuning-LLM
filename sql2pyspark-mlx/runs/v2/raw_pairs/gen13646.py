from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([3, 9, 10]))
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_accounts
        .filter(F.col("balance") > 42000)
        .select("status", F.lit("account").alias("source"))
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
